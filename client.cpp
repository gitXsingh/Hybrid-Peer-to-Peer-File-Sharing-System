// client.cpp
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <fstream>
#include <thread>
#include <algorithm> // For std::min
#include <cstring>   // For strlen, strcpy

#define BUFLEN 1024
#define FILE_CHUNK_SIZE 8192

int trackerSock;
bool loggedIn = false;
std::string currentUser;
int clientPort = 9000; // Default port for listening to peer connections
bool serverRunning = false;
pthread_t serverThreadId;
pthread_mutex_t downloadLock; // Linux synchronization mechanism

// File transfer tracking
struct DownloadInfo {
    std::string filename;
    std::string groupId;
    std::string sourcePeer;
    bool isCompleted;
    int totalSize;
    int downloadedSize;
};

std::map<std::string, DownloadInfo> downloads;

// Peer info
struct PeerInfo {
    std::string username;
    std::string ip;
    int port;
};

// Forward declarations of functions
void displayHelp();
void showDownloads();
std::string sendToTrackerAndGetResponse(const std::string &message);
void sendToTracker(const std::string &message);
bool sendFile(const std::string& filePath, int peerSock);
bool receiveFile(const std::string& savePath, int peerSock, const std::string& filename, const std::string& groupId);
void downloadFile(const std::string& groupId, const std::string& filename, const std::string& savePath);
void* handleClient(void* arg);
void* peerServerFunc(void* arg);
void startPeerServer();
void stopPeerServer();

// Custom string duplication function
char* duplicateString(const char* str) {
    size_t len = strlen(str) + 1;
    char* dup = new char[len];
    strcpy(dup, str);
    return dup;
}

// Thread function for downloads
struct DownloadParams {
    char* groupId;
    char* filename;
    char* savePath;
};

void* downloadThreadFunc(void* arg) {
    DownloadParams* params = (DownloadParams*)arg;
    downloadFile(params->groupId, params->filename, params->savePath);
    
    // Free allocated memory
    delete[] params->groupId;
    delete[] params->filename;
    delete[] params->savePath;
    delete params;
    
    return NULL;
}

// Send message to tracker and get response
std::string sendToTrackerAndGetResponse(const std::string &message) {
    send(trackerSock, message.c_str(), message.length(), 0);
    char buffer[BUFLEN];
    int recv_len = recv(trackerSock, buffer, BUFLEN, 0);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        return std::string(buffer);
    }
    return "";
}

void sendToTracker(const std::string &message) {
    std::string response = sendToTrackerAndGetResponse(message);
    
    // Update login status if needed
    if (message.find("login") == 0 && response == "Login successful") {
        // Extract username from login command
        std::istringstream iss(message);
        std::string cmd, username;
        iss >> cmd >> username;
        currentUser = username;
        loggedIn = true;
        
        // Auto-register peer after successful login
        std::string registerCmd = "register_peer " + std::to_string(clientPort);
        std::string regResponse = sendToTrackerAndGetResponse(registerCmd);
        std::cout << "Peer registration: " << regResponse << std::endl;
    } else if (message == "logout" && response == "Logged out successfully") {
        loggedIn = false;
        currentUser = "";
    }
    
    std::cout << "Tracker: " << response << std::endl;
}

// File transfer functions
bool sendFile(const std::string& filePath, int peerSock) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        // Try current directory as fallback
        std::string filename = filePath.substr(filePath.find_last_of("/\\") + 1);
        file.open(filename, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Could not open file" << std::endl;
            return false;
        }
    }
    
    // Get file size
    file.seekg(0, std::ios::end);
    int fileSize = file.tellg();
    file.seekg(0, std::ios::beg);
    
    // Send file size first
    std::string sizeMsg = std::to_string(fileSize);
    send(peerSock, sizeMsg.c_str(), sizeMsg.length(), 0);
    
    // Wait for acknowledgment
    char ackBuffer[BUFLEN];
    recv(peerSock, ackBuffer, BUFLEN, 0);
    
    // Send file data in chunks
    char buffer[FILE_CHUNK_SIZE];
    int totalSent = 0;
    int progress = 0;
    
    while (!file.eof()) {
        file.read(buffer, FILE_CHUNK_SIZE);
        int bytesRead = file.gcount();
        
        if (bytesRead > 0) {
            int bytesSent = send(peerSock, buffer, bytesRead, 0);
            if (bytesSent <= 0) {
                std::cerr << "Error sending file data" << std::endl;
                file.close();
                return false;
            }
            
            totalSent += bytesSent;
            
            // Display progress
            int newProgress = (totalSent * 100) / fileSize;
            if (newProgress >= progress + 10) {  // Update every 10%
                progress = newProgress;
                std::cout << "Upload progress: " << progress << "%" << std::endl;
            }
        }
    }
    
    file.close();
    std::cout << "Upload complete" << std::endl;
    return true;
}

bool receiveFile(const std::string& savePath, int peerSock, const std::string& filename, const std::string& groupId) {
    try {
        // Receive file size
        char sizeBuffer[BUFLEN];
        int recv_len = recv(peerSock, sizeBuffer, BUFLEN, 0);
        if (recv_len <= 0) {
            std::cerr << "Error receiving file size" << std::endl;
            return false;
        }
        
        sizeBuffer[recv_len] = '\0';
        
        // Try to parse file size with error handling
        int fileSize = 0;
        try {
            fileSize = std::stoi(sizeBuffer);
        } catch (const std::exception& e) {
            std::cerr << "Error parsing file size" << std::endl;
            return false;
        }
        
        // Send acknowledgment
        send(peerSock, "ACK", 3, 0);
        
        // Open file for writing
        std::ofstream fileStream(savePath, std::ios::binary);
        if (!fileStream) {
            std::cerr << "Error creating file: " << savePath << std::endl;
            return false;
        }
        
        // Receive file in chunks
        char buffer[FILE_CHUNK_SIZE];
        int totalReceived = 0;
        int progress = 0;
        
        // Update download info
        pthread_mutex_lock(&downloadLock);
        downloads[filename].totalSize = fileSize;
        pthread_mutex_unlock(&downloadLock);
        
        while (totalReceived < fileSize) {
            int bytesToReceive = std::min(FILE_CHUNK_SIZE, fileSize - totalReceived);
            int bytesRead = recv(peerSock, buffer, bytesToReceive, 0);
            
            if (bytesRead <= 0) {
                std::cerr << "Error receiving file data" << std::endl;
                fileStream.close();
                return false;
            }
            
            fileStream.write(buffer, bytesRead);
            totalReceived += bytesRead;
            
            // Update download info
            pthread_mutex_lock(&downloadLock);
            downloads[filename].downloadedSize = totalReceived;
            pthread_mutex_unlock(&downloadLock);
            
            // Display progress
            int newProgress = (totalReceived * 100) / fileSize;
            if (newProgress >= progress + 10) {  // Update every 10%
                progress = newProgress;
                std::cout << "Download progress: " << progress << "%" << std::endl;
            }
        }
        
        fileStream.close();
        
        // Mark download as complete
        pthread_mutex_lock(&downloadLock);
        downloads[filename].isCompleted = true;
        pthread_mutex_unlock(&downloadLock);
        
        std::cout << "Download complete" << std::endl;
        
        // Notify tracker about download completion
        std::string completionMsg = "download_complete " + filename + " " + groupId;
        std::string response = sendToTrackerAndGetResponse(completionMsg);
        std::cout << "Download notification: " << response << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in file receive: " << e.what() << std::endl;
        return false;
    }
}

void downloadFile(const std::string& groupId, const std::string& filename, const std::string& savePath) {
    // Get peer info from tracker
    std::string infoCmd = "get_peer_info " + groupId + " " + filename;
    std::string response = sendToTrackerAndGetResponse(infoCmd);
    
    if (response.find("ERROR") == 0) {
        std::cerr << "Error: " << response << std::endl;
        return;
    }
    
    // Parse response (username IP:PORT)
    std::istringstream iss(response);
    std::string peerUsername, peerIpPort;
    iss >> peerUsername >> peerIpPort;
    
    size_t pos = peerIpPort.find(":");
    if (pos == std::string::npos) {
        std::cerr << "Invalid peer info format" << std::endl;
        return;
    }
    
    std::string peerIp = peerIpPort.substr(0, pos);
    int peerPort;
    try {
        peerPort = std::stoi(peerIpPort.substr(pos + 1));
    } catch (const std::exception& e) {
        std::cerr << "Invalid port number" << std::endl;
        return;
    }
    
    std::cout << "Connecting to peer: " << peerUsername << " at " << peerIp << ":" << peerPort << std::endl;
    
    // Connect to peer
    int peerSock;
    struct sockaddr_in peerAddr;
    
    if ((peerSock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return;
    }
    
    peerAddr.sin_family = AF_INET;
    peerAddr.sin_port = htons(peerPort);
    
    // Convert IPv4 address from text to binary
    if (inet_pton(AF_INET, peerIp.c_str(), &peerAddr.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported" << std::endl;
        close(peerSock);
        return;
    }
    
    // Connect to peer
    if (connect(peerSock, (struct sockaddr *)&peerAddr, sizeof(peerAddr)) < 0) {
        std::cerr << "Connection Failed" << std::endl;
        close(peerSock);
        return;
    }
    
    // Add download info
    pthread_mutex_lock(&downloadLock);
    DownloadInfo info;
    info.filename = filename;
    info.groupId = groupId;
    info.sourcePeer = peerUsername;
    info.isCompleted = false;
    info.totalSize = 0;
    info.downloadedSize = 0;
    downloads[filename] = info;
    pthread_mutex_unlock(&downloadLock);
    
    // Send download request: "download <groupId> <filename>"
    std::string downloadRequest = "download " + groupId + " " + filename;
    send(peerSock, downloadRequest.c_str(), downloadRequest.length(), 0);
    
    // Receive and save file
    bool success = receiveFile(savePath, peerSock, filename, groupId);
    
    // Close connection
    close(peerSock);
    
    if (!success) {
        std::cerr << "Download failed" << std::endl;
    }
}

void* handleClient(void* arg) {
    int* sock_ptr = (int*)arg;
    int sock = *sock_ptr;
    delete sock_ptr;
    
    char buffer[BUFLEN];
    int len = recv(sock, buffer, BUFLEN, 0);
    if (len > 0) {
        buffer[len] = '\0';
        std::string request(buffer);
        std::cout << "Received request from peer: " << request << std::endl;
        
        std::istringstream iss(request);
        std::string command;
        iss >> command;
        
        if (command == "download") {
            std::string groupId, filename;
            iss >> groupId >> filename;
            
            // Get file path from tracker
            std::string filePathCmd = "get_file_path " + groupId + " " + filename;
            std::string filePath = sendToTrackerAndGetResponse(filePathCmd);
            
            if (filePath.find("ERROR") == 0) {
                std::cerr << "Error: " << filePath << std::endl;
            } else {
                std::cout << "Sending file: " << filePath << std::endl;
                sendFile(filePath, sock);
            }
        }
    }
    
    close(sock);
    return NULL;
}

void* peerServerFunc(void* arg) {
    // Create socket
    int serverSock, clientSock;
    struct sockaddr_in server, client;
    int opt = 1;
    socklen_t addrlen = sizeof(client);
    
    // Creating socket file descriptor
    if ((serverSock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return NULL;
    }
    
    // Set socket options
    if (setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "setsockopt error" << std::endl;
        return NULL;
    }
    
    // Set up server address
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(clientPort);
    
    // Bind socket
    if (bind(serverSock, (struct sockaddr *)&server, sizeof(server)) < 0) {
        std::cerr << "Bind failed" << std::endl;
        return NULL;
    }
    
    // Listen for connections
    if (listen(serverSock, 5) < 0) {
        std::cerr << "Listen error" << std::endl;
        return NULL;
    }
    
    std::cout << "Peer server started on port " << clientPort << std::endl;
    serverRunning = true;
    
    // Accept connections and handle them
    while (serverRunning) {
        clientSock = accept(serverSock, (struct sockaddr *)&client, &addrlen);
        if (clientSock < 0) {
            if (serverRunning) { // Only log error if server is still meant to be running
                std::cerr << "Accept failed" << std::endl;
            }
            continue;
        }
        
        // Create thread to handle client
        pthread_t clientThread;
        int* sock_ptr = new int(clientSock);
        
        if (pthread_create(&clientThread, NULL, handleClient, sock_ptr) != 0) {
            std::cerr << "Error creating thread" << std::endl;
            delete sock_ptr;
            close(clientSock);
        } else {
            // Detach thread
            pthread_detach(clientThread);
        }
    }
    
    close(serverSock);
    return NULL;
}

void startPeerServer() {
    if (serverRunning) {
        std::cout << "Peer server already running" << std::endl;
        return;
    }
    
    // Initialize mutex
    pthread_mutex_init(&downloadLock, NULL);
    
    // Create server thread
    if (pthread_create(&serverThreadId, NULL, peerServerFunc, NULL) != 0) {
        std::cerr << "Error creating server thread" << std::endl;
        return;
    }
}

void stopPeerServer() {
    if (!serverRunning) {
        return;
    }
    
    serverRunning = false;
    
    // Create temporary socket to unblock accept
    int tempSock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(clientPort);
    
    // Convert localhost to binary
    inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);
    
    // Connect to server to unblock accept
    connect(tempSock, (struct sockaddr*)&server, sizeof(server));
    close(tempSock);
    
    // Wait for server thread to complete
    pthread_join(serverThreadId, NULL);
    
    // Destroy mutex
    pthread_mutex_destroy(&downloadLock);
    
    std::cout << "Peer server stopped" << std::endl;
}

void displayHelp() {
    std::cout << "Available commands:" << std::endl;
    std::cout << "create_user <username> <password> - Create a new user" << std::endl;
    std::cout << "login <username> <password> - Login with credentials" << std::endl;
    std::cout << "create_group <group_id> - Create a new group" << std::endl;
    std::cout << "join_group <group_id> - Request to join a group" << std::endl;
    std::cout << "leave_group <group_id> - Leave a group" << std::endl;
    std::cout << "list_requests <group_id> - List pending join requests for a group" << std::endl;
    std::cout << "accept_request <group_id> <username> - Accept a user's join request" << std::endl;
    std::cout << "list_groups - List all groups" << std::endl;
    std::cout << "list_files <group_id> - List all files in a group" << std::endl;
    std::cout << "upload_file <file_path> <group_id> - Share a file in a group" << std::endl;
    std::cout << "download_file <group_id> <filename> <destination_path> - Download a file" << std::endl;
    std::cout << "show_downloads - Show status of downloads" << std::endl;
    std::cout << "stop_share <group_id> <filename> - Stop sharing a file" << std::endl;
    std::cout << "logout - Logout from current session" << std::endl;
    std::cout << "exit - Exit the application" << std::endl;
}

void showDownloads() {
    pthread_mutex_lock(&downloadLock);
    if (downloads.empty()) {
        std::cout << "No downloads" << std::endl;
    } else {
        std::cout << "Downloads:" << std::endl;
        for (const auto& download : downloads) {
            const DownloadInfo& info = download.second;
            std::cout << "File: " << info.filename << ", Group: " << info.groupId << ", Source: " << info.sourcePeer;
            
            if (info.isCompleted) {
                std::cout << " [COMPLETED]" << std::endl;
            } else {
                float progress = (info.totalSize > 0) ? 
                                 ((float)info.downloadedSize / info.totalSize * 100) : 0;
                std::cout << " [" << progress << "%]" << std::endl;
            }
        }
    }
    pthread_mutex_unlock(&downloadLock);
}

void commandLoop() {
    std::string input;
    while (true) {
        std::cout << "> ";
        std::getline(std::cin, input);
        
        if (input == "exit" || input == "quit") {
            break;
        } else if (input == "help") {
            displayHelp();
        } else if (input == "show_downloads") {
            showDownloads();
        } else if (input.find("upload_file") == 0) {
            if (!loggedIn) {
                std::cout << "Please login first" << std::endl;
                continue;
            }
            
            std::istringstream iss(input);
            std::string cmd, filepath, groupId;
            iss >> cmd >> filepath >> groupId;
            
            if (filepath.empty() || groupId.empty()) {
                std::cout << "Usage: upload_file <file_path> <group_id>" << std::endl;
                continue;
            }
            
            // Check if file exists
            std::ifstream fileCheck(filepath);
            if (!fileCheck) {
                std::cout << "File not found: " << filepath << std::endl;
                continue;
            }
            fileCheck.close();
            
            // Send upload command to tracker
            std::string filename = filepath.substr(filepath.find_last_of("/\\") + 1);
            std::string uploadCmd = "upload_file " + filepath + " " + groupId;
            sendToTracker(uploadCmd);
        } else if (input.find("download_file") == 0) {
            if (!loggedIn) {
                std::cout << "Please login first" << std::endl;
                continue;
            }
            
            std::istringstream iss(input);
            std::string cmd, groupId, filename, savePath;
            iss >> cmd >> groupId >> filename >> savePath;
            
            if (groupId.empty() || filename.empty() || savePath.empty()) {
                std::cout << "Usage: download_file <group_id> <filename> <destination_path>" << std::endl;
                continue;
            }
            
            // Create and run download thread
            DownloadParams* params = new DownloadParams;
            params->groupId = duplicateString(groupId.c_str());
            params->filename = duplicateString(filename.c_str());
            params->savePath = duplicateString(savePath.c_str());
            
            pthread_t downloadThread;
            pthread_create(&downloadThread, NULL, downloadThreadFunc, params);
            pthread_detach(downloadThread);
        } else {
            // Forward command to tracker
            sendToTracker(input);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <tracker_ip:port>" << std::endl;
        return 1;
    }
    
    std::string ip_port(argv[1]);
    size_t pos = ip_port.find(":");
    if (pos == std::string::npos) {
        std::cerr << "Invalid format. Use: <tracker_ip:port>" << std::endl;
        return 1;
    }
    
    std::string tracker_ip = ip_port.substr(0, pos);
    int tracker_port;
    try {
        tracker_port = std::stoi(ip_port.substr(pos + 1));
    } catch (const std::exception& e) {
        std::cerr << "Invalid port number" << std::endl;
        return 1;
    }
    
    // Create socket
    if ((trackerSock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return 1;
    }
    
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(tracker_port);
    
    // Convert IPv4 address from text to binary
    if (inet_pton(AF_INET, tracker_ip.c_str(), &serv_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported" << std::endl;
        return 1;
    }
    
    // Connect to tracker
    if (connect(trackerSock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Connection Failed" << std::endl;
        return 1;
    }
    
    std::cout << "Connected to tracker at " << tracker_ip << ":" << tracker_port << std::endl;
    
    // Start peer server
    startPeerServer();
    
    // Display help information
    displayHelp();
    
    // Enter command loop
    commandLoop();
    
    // Cleanup
    stopPeerServer();
    close(trackerSock);
    
    return 0;
}
