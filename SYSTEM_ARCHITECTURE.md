# System Architecture Diagram - Hybrid P2P File Sharing System (Verified)

This is the single consolidated architecture document for the project. It includes:
- Verified detailed diagrams
- Dark-theme presentation variants
- SVG-export-ready simplified variants

---

## Diagram Verification Status

- **Architecture flowchart:** Correct for current codebase (`client.cpp`, `tracker.cpp`).
- **Request lifecycle sequence:** Correct, with one highlighted implementation mismatch in `download_complete` argument order.
- **ER model:** Correct conceptual mapping of in-memory structures.
- **State diagram:** Removed to reduce clutter (not required for architecture understanding).

---

## 1) End-to-End Architecture (Flowchart)

```mermaid
flowchart LR
%% Hybrid P2P architecture: centralized control plane + decentralized data plane

%% =========================
%% Client Layer
%% =========================
subgraph CL["Client Layer (Entry + UX)"]
  U["User\nCLI Input / Commands"]
  C1["Client Peer A\n(UI Command Loop + Peer Server + Downloader)"]
  C2["Client Peer B\n(File Owner / Seeder)"]
end

%% =========================
%% Server Layer
%% =========================
subgraph SL["Server Layer (Coordination + Business Logic)"]
  LB["TCP Load Balancer (Optional)\nDistributes tracker traffic"]
  T1["Tracker API Instance #1\nAuth + Group Mgmt + File Metadata + Peer Discovery"]
  T2["Tracker API Instance #2 (Standby/Scale)\nFailover / Horizontal Scale"]
  AUTH["Auth Module\ncreate_user/login/logout"]
  GRP["Group Module\ncreate/join/accept/list"]
  META["Metadata Module\nupload/list/stop_share"]
  DISC["Discovery Module\nget_peer_info/get_file_path"]
  DL["Download Status Module\nshow_downloads/download_complete"]
  Q["Async Event Queue (Optional)\nAudit/analytics/events"]
  W["Background Worker (Optional)\nConsumes async events"]
end

%% =========================
%% Data Layer
%% =========================
subgraph DLAYER["Data Layer"]
  CACHE["In-Memory Cache / Hot State\npeerInfo, sessions, lookup maps"]
  MEMDB["Primary Runtime Store (Current)\nIn-process maps in tracker.cpp"]
  SQL["SQL DB (Optional Persistent Store)\nusers, groups, files, memberships, downloads"]
  NOSQL["NoSQL/Event Store (Optional)\ntelemetry, logs, activity stream"]
end

%% =========================
%% External Services
%% =========================
subgraph EX["External Services / Infrastructure"]
  NET["OS Networking Stack\nWinsock/TCP sockets"]
  OBS["Monitoring & Logging (Optional)\nmetrics, health, traces"]
  DNS["DNS / Service Discovery (Optional)\ntracker endpoint resolution"]
end

%% Entry and command path
U -->|CLI Command| C1
C1 -->|TCP Command (sync request)| LB
LB -->|Route Request| T1
LB -->|Failover Route| T2

%% Module decomposition inside tracker
T1 --> AUTH
T1 --> GRP
T1 --> META
T1 --> DISC
T1 --> DL

%% Data interactions
AUTH -->|Read/Write user credentials| MEMDB
GRP -->|Read/Write groups + membership| MEMDB
META -->|Read/Write file records| MEMDB
DISC -->|Resolve owner IP:port + file path| MEMDB
DL -->|Update download completion flags| MEMDB
T1 -->|Fast lookup| CACHE
CACHE -->|Cache miss/fill| MEMDB

%% Optional persistence and analytics
MEMDB -.->|Persist snapshots/events (async)| SQL
T1 -.->|Emit events (upload/download/login)| Q
Q -.->|Consume events| W
W -.->|Store analytics/logs| NOSQL

%% Peer discovery and direct data plane
C1 -->|get_peer_info(groupId,fileName)| T1
T1 -->|ownerUsername + ownerIP:ownerPort| C1
C1 -->|TCP direct connect + "download groupId fileName"| C2
C2 -->|File chunks (8KB stream)| C1

%% Output delivery
C1 -->|Progress + final status to user| U
C1 -->|download_complete filename groupId (current implementation)| T1

%% Infrastructure connections
T1 -->|Socket I/O| NET
C1 -->|Socket I/O| NET
C2 -->|Socket I/O| NET
LB -->|Health checks| T1
LB -->|Health checks| T2
T1 -.->|Metrics/logs| OBS
LB -.->|Endpoint resolution| DNS

%% Styling classes
classDef frontend fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a,stroke-width:1.5px;
classDef backend fill:#dcfce7,stroke:#15803d,color:#14532d,stroke-width:1.5px;
classDef database fill:#ffedd5,stroke:#c2410c,color:#7c2d12,stroke-width:1.5px;
classDef external fill:#f3e8ff,stroke:#7e22ce,color:#581c87,stroke-width:1.5px;
classDef critical fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d,stroke-width:2px;

class U,C1,C2 frontend;
class LB,T1,T2,AUTH,GRP,META,DISC,DL,Q,W backend;
class CACHE,MEMDB,SQL,NOSQL database;
class NET,OBS,DNS external;
class C1,T1,C2 critical;

%% Interactive clickable nodes
click C1 "client.cpp" "Client app: command loop, peer server, downloader"
click T1 "tracker.cpp" "Tracker server: auth, groups, metadata, discovery"
click MEMDB "tracker.cpp" "In-memory maps used as runtime data store"
click U "README.md#available-commands" "CLI commands exposed to users"
click DISC "README.md#file-transfer-process" "Peer discovery and transfer process"
```

---

## 2) Request Lifecycle (Sequence Diagram)

```mermaid
sequenceDiagram
autonumber
participant User as User (CLI)
participant ClientA as Client Peer A
participant Tracker as Tracker API
participant ClientB as Client Peer B (Owner)
participant Store as Runtime Store (Maps)

Note over User,ClientA: Entry point: user starts download_file <group_id> <file> <dest>
User->>ClientA: download_file(groupId, fileName, destinationPath)
ClientA->>Tracker: get_peer_info groupId fileName (TCP sync)
Tracker->>Store: validate member + resolve owner + endpoint
Store-->>Tracker: ownerUsername, ownerIP:ownerPort
Tracker-->>ClientA: peer_info response

Note over ClientA,ClientB: Critical path: decentralized data transfer
ClientA->>ClientB: TCP connect + "download groupId fileName"
ClientB->>Tracker: get_file_path groupId fileName
Tracker->>Store: resolve owner file path
Store-->>Tracker: absolute/registered file path
Tracker-->>ClientB: file path response
ClientB-->>ClientA: fileSize
ClientA-->>ClientB: ACK
loop Until file complete
  ClientB-->>ClientA: file chunk (8KB)
  ClientA->>ClientA: update DownloadInfo(progress,total)
end
ClientA->>Tracker: download_complete filename groupId
Note over ClientA,Tracker: Implementation note: tracker expects groupId first, filename second
Tracker->>Store: marks downloads[username][args[1]] (currently groupId due to mismatch)
Tracker-->>ClientA: status updated
ClientA-->>User: progress + completion output
```

---

## 3) Data Model (ER Diagram)

```mermaid
erDiagram
  USER {
    string username PK
    string password
  }

  GROUP_ENTITY {
    string groupId PK
    string ownerUsername FK
  }

  GROUP_MEMBERSHIP {
    string groupId FK
    string username FK
    string status "member|pending"
  }

  FILE_INFO {
    string filename PK
    string ownerUsername FK
    string filePath
    int sizeBytes
  }

  FILE_GROUP_MAP {
    string filename FK
    string groupId FK
  }

  PEER_INFO {
    string username PK
    string ip
    int port
  }

  DOWNLOAD_STATUS {
    string username FK
    string filename FK
    bool isComplete
  }

  USER ||--o{ GROUP_ENTITY : owns
  USER ||--o{ GROUP_MEMBERSHIP : participates_in
  GROUP_ENTITY ||--o{ GROUP_MEMBERSHIP : has
  USER ||--o{ FILE_INFO : shares
  FILE_INFO ||--o{ FILE_GROUP_MAP : scoped_to
  GROUP_ENTITY ||--o{ FILE_GROUP_MAP : contains
  USER ||--|| PEER_INFO : publishes_endpoint
  USER ||--o{ DOWNLOAD_STATUS : tracks
  FILE_INFO ||--o{ DOWNLOAD_STATUS : downloaded_as
```

---

## 4) Legend and Conventions

```mermaid
flowchart LR
  FE["Frontend / Client"]:::frontend
  BE["Backend Service"]:::backend
  DB["Database / Storage"]:::database
  EX["External / Infra"]:::external
  CR["Critical Path Node"]:::critical

  A["Sync Interaction"] -->|Solid Arrow| B["Request/Response"]
  C["Async Interaction"] -.->|Dotted Arrow| D["Queue/Event/Telemetry"]

  classDef frontend fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a,stroke-width:1.5px;
  classDef backend fill:#dcfce7,stroke:#15803d,color:#14532d,stroke-width:1.5px;
  classDef database fill:#ffedd5,stroke:#c2410c,color:#7c2d12,stroke-width:1.5px;
  classDef external fill:#f3e8ff,stroke:#7e22ce,color:#581c87,stroke-width:1.5px;
  classDef critical fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d,stroke-width:2px;
```

---

## 5) Bottlenecks and Reliability Notes

- **Critical bottleneck:** single tracker process with in-memory state (no durable replication by default).
- **Current reliability profile:** tracker restart clears runtime state; peer data path remains decentralized.
- **Scale-out path shown in diagram:** load balancer + second tracker + optional SQL persistence + async queue.
- **Throughput behavior:** tracker handles lightweight metadata only, while bulk file transfer scales with number of peers.
- **Observed implementation inconsistency:** `client.cpp` sends `download_complete <filename> <groupId>`, while `tracker.cpp` parses as `<groupId> <filename>`.

---

## 6) Dark Theme Variants (Presentation)

### 6.1 Architecture Overview (Dark)

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#0f172a', 'primaryTextColor': '#e2e8f0', 'lineColor': '#94a3b8', 'tertiaryColor': '#111827' }}}%%
flowchart LR
subgraph CL["Client Layer"]
  U["User CLI"]
  C1["Client Peer A\n(Command Loop + Peer Server)"]
  C2["Client Peer B\n(File Owner)"]
end

subgraph SL["Server Layer"]
  LB["Optional TCP LB"]
  T["Tracker Server\n(Auth + Groups + Metadata + Discovery)"]
  Q["Optional Async Queue"]
end

subgraph DL["Data Layer"]
  MEM["In-memory Runtime Store\n(maps in tracker.cpp)"]
  SQL["Optional SQL Persistence"]
end

subgraph EX["External / Infra"]
  NET["Winsock / TCP Stack"]
  OBS["Optional Monitoring"]
end

U -->|CLI command| C1
C1 -->|TCP request/response| LB --> T
T -->|Read/Write| MEM
MEM -.->|Optional persistence| SQL
C1 -->|get_peer_info| T
T -->|owner IP:port| C1
C1 -->|Direct TCP download| C2
C2 -->|8KB file chunks| C1
C1 -->|download_complete filename groupId| T
T -.->|Events| Q
T --> NET
C1 --> NET
C2 --> NET
T -.-> OBS

classDef frontend fill:#1e3a8a,stroke:#93c5fd,color:#dbeafe;
classDef backend fill:#14532d,stroke:#86efac,color:#dcfce7;
classDef database fill:#7c2d12,stroke:#fdba74,color:#ffedd5;
classDef external fill:#581c87,stroke:#d8b4fe,color:#f3e8ff;

class U,C1,C2 frontend;
class LB,T,Q backend;
class MEM,SQL database;
class NET,OBS external;

click C1 "client.cpp" "Client-side command and transfer logic"
click T "tracker.cpp" "Tracker metadata and coordination logic"
click MEM "tracker.cpp" "In-memory data structures"
```

### 6.2 Download Lifecycle (Dark)

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
autonumber
participant User as User
participant CA as Client A
participant TR as Tracker
participant CB as Client B

User->>CA: download_file(groupId, fileName, savePath)
CA->>TR: get_peer_info groupId fileName
TR-->>CA: ownerUsername + ownerIP:ownerPort
CA->>CB: TCP connect + download groupId fileName
CB->>TR: get_file_path groupId fileName
TR-->>CB: file path
CB-->>CA: fileSize
CA-->>CB: ACK
loop stream
  CB-->>CA: 8KB chunk
end
CA->>TR: download_complete filename groupId
Note over CA,TR: Current code mismatch: tracker parses args as groupId,filename
TR-->>CA: Download status updated
CA-->>User: Progress + completion
```

---

## 7) SVG Export Ready Variants (Simplified)

### 7.1 Core System Architecture (SVG)

```mermaid
flowchart LR
subgraph Client["Client Layer"]
  U["User (CLI)"]
  CA["Client Peer A"]
  CB["Client Peer B (Owner)"]
end

subgraph Server["Server Layer"]
  T["Tracker Server\nAuth + Groups + Metadata + Discovery"]
end

subgraph Data["Data Layer"]
  M["In-memory Store\nusers/groups/files/peerInfo/downloads"]
end

U -->|Input command| CA
CA -->|TCP metadata request| T
T -->|Metadata response| CA
T -->|Read/Write state| M
CA -->|Direct TCP download request| CB
CB -->|File stream (8KB chunks)| CA
CA -->|Progress + result| U
CA -->|download_complete filename groupId| T
```

### 7.2 Download Request-Response Cycle (SVG)

```mermaid
sequenceDiagram
autonumber
participant U as User
participant A as Client A
participant T as Tracker
participant B as Client B

U->>A: download_file(groupId, fileName, savePath)
A->>T: get_peer_info(groupId, fileName)
T-->>A: ownerIP:ownerPort
A->>B: connect + download(groupId, fileName)
B->>T: get_file_path(groupId, fileName)
T-->>B: file path
B-->>A: file size
A-->>B: ACK
loop file transfer
  B-->>A: file chunk
end
A->>T: download_complete filename groupId
T-->>A: status updated
A-->>U: download complete
```

### 7.3 Data Relationships (SVG)

```mermaid
erDiagram
  USER ||--o{ GROUP_MEMBERSHIP : member_of
  GROUP ||--o{ GROUP_MEMBERSHIP : contains
  USER ||--o{ FILE_INFO : owns
  FILE_INFO ||--o{ FILE_GROUP_MAP : shared_in
  GROUP ||--o{ FILE_GROUP_MAP : has
  USER ||--|| PEER_INFO : advertises
  USER ||--o{ DOWNLOAD_STATUS : has
```

### 7.4 Export Tips

- Use Mermaid live editor or VS Code Mermaid preview to export SVG.
- Keep 16:9 canvas for architecture diagrams.
- Export diagrams individually for slide decks.

