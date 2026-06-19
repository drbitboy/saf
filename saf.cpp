#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <mutex>
#include <sqlite3.h>
#include <pugixml.hpp>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

const std::string DB_NAME = "indi_forward_queue.db";
const int LISTEN_PORT = 7624;
const std::string REMOTE_HOST = "192.168.1.50";
const int REMOTE_PORT = 7624;

// Mutex prevents race conditions on database operations
std::mutex db_mutex;

void init_db() {
  sqlite3* db;
  if (sqlite3_open(DB_NAME.c_str(), &db) != SQLITE_OK) {
    std::cerr << "[-] Failed to open database.\n";
    return;
  }
  
  // Composite Unique Key handles our automatic overwrite logic
  std::string create_table = 
    "CREATE TABLE IF NOT EXISTS store_forward_queue ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  root_name TEXT,"
    "  device_name TEXT,"
    "  label_name TEXT,"
    "  payload TEXT,"
    "  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,"
    "  UNIQUE(root_name, device_name, label_name)"
    ");";
      
  char* err_msg = nullptr;
  if (sqlite3_exec(db, create_table.c_str(), 
                   nullptr, nullptr, &err_msg) != SQLITE_OK) {
    std::cerr << "[-] SQLite Schema Error: " << err_msg << "\n";
    sqlite3_free(err_msg);
  } else {
    std::cout << "[+] Database initialized with constraints.\n";
  }
  sqlite3_close(db);
}

void process_and_store_xml(const std::string& xml_string) {
  pugi::xml_document doc;
  // Parse using fragment configurations for stream chunks
  pugi::xml_parse_result result = doc.load_string(
    xml_string.c_str(), 
    pugi::parse_default | pugi::parse_fragment
  );
  
  if (!result) {
    std::cerr << "[-] Error parsing completed XML block.\n";
    return;
  }
  
  // Extract unique keys using standard INDI protocol attributes
  pugi::xml_node root = doc.first_child();
  std::string root_name = root.name();
  std::string device_name = root.attribute("device").as_string();
  std::string label_name = root.attribute("name").as_string();
  
  if (root_name.empty() || device_name.empty() || label_name.empty()) {
    return; 
  }
  
  std::lock_guard<std::mutex> lock(db_mutex);
  sqlite3* db;
  if (sqlite3_open(DB_NAME.c_str(), &db) == SQLITE_OK) {
    // INSERT OR REPLACE removes old records if keys match
    std::string insert_sql = 
      "INSERT OR REPLACE INTO store_forward_queue "
      "(root_name, device_name, label_name, payload, timestamp) "
      "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP);";
          
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db, insert_sql.c_str(), -1, 
                           &stmt, nullptr) == SQLITE_OK) {
      sqlite3_bind_text(stmt, 1, root_name.c_str(), -1,
                        SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 2, device_name.c_str(), -1,
                        SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 3, label_name.c_str(), -1,
                        SQLITE_TRANSIENT);
      sqlite3_bind_text(stmt, 4, xml_string.c_str(), -1,
                        SQLITE_TRANSIENT);
          
      if (sqlite3_step(stmt) == SQLITE_DONE) {
        std::cout << "[~] Local DB Updated: [" << root_name 
                  << " -> " << device_name << " -> " 
                  << label_name << "]\n";
      }
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
  }
}
void handle_indi_client_connection(int client_socket) {
  std::string stream_buffer = "";
  char ch;
  int depth = 0;
  bool inside_tag = false;
  std::string current_tag = "";
  bool is_closing_tag = false;
  
  // Read the stream byte-by-byte to track element boundaries
  while (true) {
    int bytes_received = recv(client_socket, &ch, 1, 0);
    if (bytes_received <= 0) break; 
    
    stream_buffer += ch;
    
    if (ch == '<') {
      inside_tag = true;
      current_tag = "";
      is_closing_tag = false;
      continue;
    }
    
    if (inside_tag) {
      if (ch == '>') {
        inside_tag = false;
        
        size_t len = stream_buffer.length();
        // Check if current tag is self-closing
        bool is_self_closing = (len >= 2 && 
                               stream_buffer[len - 2] == '/');
        
        if (is_closing_tag) {
          depth--;
        } else if (!is_self_closing) {
          // Ignore metadata tags such as <?xml ... ?>
          if (!current_tag.empty() && 
              current_tag != "?" && current_tag != "!") {
            depth++;
          }
        }
        
        // If tag balance reaches zero, process structural chunk
        if (depth == 0 && !stream_buffer.empty()) {
          while(!stream_buffer.empty() && 
                (stream_buffer.front() == '\n' || 
                 stream_buffer.front() == '\r' || 
                 stream_buffer.front() == ' ')) {
            stream_buffer.erase(0, 1);
          }
          if (!stream_buffer.empty()) {
            process_and_store_xml(stream_buffer);
          }
          stream_buffer.clear();
        }
      } else {
        if (current_tag.empty() && ch == '/') {
          is_closing_tag = true;
        } else if (ch != ' ' && ch != '/') {
          current_tag += ch; 
        }
      }
    }
  }
  close(client_socket);
}

void background_forwarder_loop() {
  std::cout << "[*] Resilient dispatcher active.\n";
  
  while (true) {
    sqlite3* db;
    if (sqlite3_open(DB_NAME.c_str(), &db) != SQLITE_OK) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }
    
    // Fetch the oldest stored pending record
    std::string select_sql = "SELECT id, payload "
                             "FROM store_forward_queue "
                             "ORDER BY id ASC LIMIT 1;";
    sqlite3_stmt* stmt;
    int message_id = -1;
    std::string payload = "";
    bool has_data = false;
    
    {
      std::lock_guard<std::mutex> lock(db_mutex);
      if (sqlite3_prepare_v2(db, select_sql.c_str(), -1, 
                             &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
          message_id = sqlite3_column_int(stmt, 0);
          payload = reinterpret_cast<const char*>(
            sqlite3_column_text(stmt, 1)
          );
          has_data = true;
        }
      }
      sqlite3_finalize(stmt);
    }
    
    if (!has_data) {
      sqlite3_close(db);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }
    
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      sqlite3_close(db);
      std::this_thread::sleep_for(std::chrono::seconds(5));
      continue;
    }
    
    // Short timeouts stop thread execution hanging while offline
    struct timeval tv;
    tv.tv_sec = 5;  
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,
               (const char*)&tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
               (const char*)&tv, sizeof(tv));
    
    struct sockaddr_in remote_addr;
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(REMOTE_PORT);
    inet_pton(AF_INET, REMOTE_HOST.c_str(), &remote_addr.sin_addr);
    
    // Attempt to cross the communication line
    if (connect(sock, (struct sockaddr*)&remote_addr, 
                sizeof(remote_addr)) >= 0) {
      std::string transmission_data = payload + "\n";
      int bytes_sent = send(sock, transmission_data.c_str(), 
                            transmission_data.length(), 0);
      
      char response_buffer = {0};
      int bytes_received = recv(sock, &response_buffer, 1, 0); 
      
      // Delete record only after receiving validation feedback
      if (bytes_sent > 0 && bytes_received > 0) {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::string delete_sql = "DELETE FROM "
                                 "store_forward_queue "
                                 "WHERE id = ?;";
        sqlite3_stmt* del_stmt;
        if (sqlite3_prepare_v2(db, delete_sql.c_str(), -1, 
                               &del_stmt, nullptr) == SQLITE_OK) {
          sqlite3_bind_int(del_stmt, 1, message_id);
          sqlite3_step(del_stmt);
        }
        sqlite3_finalize(del_stmt);
        std::cout << "[->] Forwarded message ID " << message_id 
                  << ". Removed from backlog.\n";
        
        close(sock);
        sqlite3_close(db);
        // Keep loop running fast to clear backlogs when online
        std::this_thread::sleep_for(
          std::chrono::milliseconds(10)
        ); 
        continue;
      } else {
        std::cerr << "[-] Handshake failed. Keeping message.\n";
      }
    } else {
      std::cerr << "[-] Medium offline. Retaining items.\n";
    }
    
    close(sock);
    sqlite3_close(db);
    // Backoff throttle keeps the script resource friendly
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

int main() {
  init_db();
  
  // Spawn sync engine background loop
  std::thread dispatcher(background_forwarder_loop);
  dispatcher.detach();
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  int socket_option = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, 
             &socket_option, sizeof(socket_option));
  
  struct sockaddr_in server_address;
  server_address.sin_family = AF_INET;
  server_address.sin_addr.s_addr = INADDR_ANY;
  server_address.sin_port = htons(LISTEN_PORT);
  
  if (bind(server_fd, (struct sockaddr*)&server_address, 
           sizeof(server_address)) < 0) {
    std::cerr << "[-] TCP Server failed to bind onto port " 
              << LISTEN_PORT << "\n";
    return -1;
  }
  
  listen(server_fd, 20);
  std::cout << "[+] Store-and-Forward listening on port " 
            << LISTEN_PORT << "...\n";
  
  while (true) {
    struct sockaddr_in client_address;
    socklen_t client_len = sizeof(client_address);
    int client_socket = accept(server_fd, 
                               (struct sockaddr*)&client_address, 
                               &client_len);
    
    if (client_socket >= 0) {
      // Spin off thread for new connected client streams
      std::thread(handle_indi_client_connection,
                  client_socket).detach();
    }
  }
  
  close(server_fd);
  return 0;
}
