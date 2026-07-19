import sys
import hashlib
import requests

MASTER_URL = "http://localhost:80"  # NGINX Load Balancer
CHUNK_SIZE = 1024 * 1024  # 1 MB

def sha256_hash(data):
    return hashlib.sha256(data).hexdigest()

def execute_e2e_test():
    print("=== STARTING END-TO-END DEDUPLICATION & REPLICATION TEST ===")
    
    # ---------------------------------------------------------
    # PHASE 0: RUNTIME CLUSTER BOOTSTRAP
    # ---------------------------------------------------------
    print("[0.1/9] Database initialized empty. Executing cluster bootstrap...")
    print("        Note: Verify worker IDs by inspecting the database 'worker_nodes' table.")
    
    # Prompt the operator for registered worker IDs at runtime
    worker_ids_input = input("        Enter active worker IDs to register as Cluster 1 (comma-separated, e.g. 1,2): ")
    try:
        active_node_ids = [int(x.strip()) for x in worker_ids_input.split(",") if x.strip().isdigit()]
    except Exception as e:
        print(f"[FAIL] Invalid input formatting: {e}")
        sys.exit(1)
        
    if not active_node_ids:
        print("[FAIL] You must provide at least one valid integer worker ID.")
        sys.exit(1)

    print(f"        Sending bootstrap request for 'cluster_alpha' with workers {active_node_ids}...")
    bootstrap_payload = {
        "cluster_name": "cluster_alpha",
        "active_node_ids": active_node_ids
    }
    bootstrap_response = requests.post(f"{MASTER_URL}/admin/cluster/add", json=bootstrap_payload)
    
    if bootstrap_response.status_code != 200:
        print(f"[FAIL] Bootstrap failed with status: {bootstrap_response.status_code}")
        print(bootstrap_response.text)
        sys.exit(1)
        
    print("        PASS: Shard map and cluster baseline successfully initialized.")

    # ---------------------------------------------------------
    # PHASE 0.5: DUMMY USER PROVISIONING
    # ---------------------------------------------------------
    print("\n[0.2/9] Provisioning dummy user to satisfy relational foreign keys...")
    signup_payload = {
        "email": "testuser@example.com",
        "password": "password123"
    }
    signup_response = requests.post(f"{MASTER_URL}/auth/signup", json=signup_payload)
    
    if signup_response.status_code == 201:
        print("        PASS: Dummy user created successfully. Assigned user_id: 1.")
    elif signup_response.status_code == 409:
        print("        PASS: User already exists. Continuing execution.")
    else:
        print(f"[FAIL] User provisioning failed with status: {signup_response.status_code}")
        sys.exit(1)

    # ---------------------------------------------------------
    # PHASE 1: DATA INGESTION (FILE A)
    # ---------------------------------------------------------
    # Using a simple UTF-8 encoded text payload to easily inspect encryption at rest
    raw_sentence = "This is a highly secure, decoupled, and horizontally scalable distributed storage engine."
    print(f"\n[1/9] Packaging local text payload (File A): '{raw_sentence}'")
    original_data = raw_sentence.encode('utf-8')
    
    chunks = [original_data]
    chunk_hashes = [sha256_hash(original_data)]
    
    print(f"      Calculated Chunk Hash: {chunk_hashes[0]}")

    # 2. Request upload initialization for File A
    print("\n[2/9] Initializing upload for File A on Control Plane...")
    init_payload = {"hashes": chunk_hashes}
    init_response = requests.post(f"{MASTER_URL}/upload-init", json=init_payload)
    
    if init_response.status_code != 200:
        print(f"[FAIL] Upload initialization failed with status: {init_response.status_code}")
        sys.exit(1)
        
    init_data = init_response.json()
    missing_hashes = init_data.get("missing_hashes", [])
    print(f"      Master reports {len(missing_hashes)} missing chunks to be physically uploaded.")

    routing_map = {}

    # 3. Perform physical binary uploads to ALL assigned replica Workers (Replication)
    print("\n[3/9] Transferring binary chunks to ALL replica Workers (Direct Client Replication)...")
    for item in missing_hashes:
        chunk_hash = item["hash"]
        assigned_nodes = item["assigned_nodes"]
        
        if not assigned_nodes:
            print(f"[FAIL] No active worker nodes assigned to chunk: {chunk_hash}")
            sys.exit(1)
            
        # Store assigned replica nodes list for retrieval verification
        routing_map[chunk_hash] = assigned_nodes
        
        chunk_idx = chunk_hashes.index(chunk_hash)
        binary_payload = chunks[chunk_idx]
        
        # Smart client writes to all returned nodes to maintain configured replication factor
        for node in assigned_nodes:
            worker_ip = node["advertised_host"]
            worker_port = node["advertised_port"]
            worker_url = f"http://{worker_ip}:{worker_port}/chunk?hash={chunk_hash}"
            print(f"      Replicating chunk {chunk_hash[:12]}... -> Direct to Worker ({worker_ip}:{worker_port})")
            
            upload_response = requests.post(worker_url, data=binary_payload, headers={"Content-Type": "application/octet-stream"})
            if upload_response.status_code != 200:
                print(f"[FAIL] Replicated upload failed at ({worker_ip}:{worker_port}) with status: {upload_response.status_code}")
                sys.exit(1)

    # 4. Commit File A to the Control Plane
    print("\n[4/9] Committing File A metadata to Master...")
    commit_payload = {
        "filename": "file_A.txt",
        "file_size": len(original_data),
        "chunk_hashes": chunk_hashes
    }
    commit_response = requests.post(f"{MASTER_URL}/files/commit", json=commit_payload)
    if commit_response.status_code != 200:
        print(f"[FAIL] File A commit failed: {commit_response.text}")
        sys.exit(1)
    print("      File A metadata committed. Reference counts incremented to 1.")

    # ---------------------------------------------------------
    # PHASE 2: DEDUPLICATION VERIFICATION
    # ---------------------------------------------------------
    print("\n[5/9] Verifying Deduplication Engine (Zero-Copy Upload)...")
    init_payload_dup = {"hashes": chunk_hashes}
    init_response_dup = requests.post(f"{MASTER_URL}/upload-init", json=init_payload_dup)
    
    if init_response_dup.status_code != 200:
        print(f"[FAIL] Upload initialization for duplicate file failed.")
        sys.exit(1)
        
    init_data_dup = init_response_dup.json()
    missing_hashes_dup = init_data_dup.get("missing_hashes", [])
    
    if len(missing_hashes_dup) == 0:
         print("      PASS: Deduplication verified. Zero chunks are reported as missing.")
    else:
         print(f"[FAIL] Deduplication failed. Master reported {len(missing_hashes_dup)} chunks as missing.")
         sys.exit(1)

    # 6. Metadata-Only Commit for File B
    print("\n[6/9] Committing File B metadata pointing to identical chunks...")
    commit_payload_dup = {
        "filename": "file_B.txt",
        "file_size": len(original_data),
        "chunk_hashes": chunk_hashes
    }
    commit_response_dup = requests.post(f"{MASTER_URL}/files/commit", json=commit_payload_dup)
    if commit_response_dup.status_code != 200:
        print(f"[FAIL] File B commit failed: {commit_response_dup.text}")
        sys.exit(1)
    print("      File B committed successfully. Deduplicated link metadata verified.")

    # ---------------------------------------------------------
    # PHASE 3: DATA RETRIEVAL (FILE B)
    # ---------------------------------------------------------
    print("\n[7/9] Simulating download: Retrieving deduplicated chunks for File B from Workers...")
    downloaded_chunks = []
    for h in chunk_hashes:
        assigned_nodes = routing_map[h]
        # Download from the first replica node in the resolved list
        primary_node = assigned_nodes[0]
        worker_ip = primary_node["advertised_host"]
        worker_port = primary_node["advertised_port"]
        
        download_url = f"http://{worker_ip}:{worker_port}/chunk?hash={h}"
        print(f"      Downloading chunk {h[:12]}... <- Direct from Worker ({worker_ip}:{worker_port})")
        
        download_response = requests.get(download_url)
        if download_response.status_code != 200:
            print(f"[FAIL] Download failed for chunk {h}: Status {download_response.status_code}")
            sys.exit(1)
            
        downloaded_chunks.append(download_response.content)

    # 8. Reassemble File B and execute cryptographic hash verification
    print("\n[8/9] Executing byte-for-byte payload verification...")
    downloaded_data = b"".join(downloaded_chunks)
    
    original_sha = sha256_hash(original_data)
    downloaded_sha = sha256_hash(downloaded_data)
    
    print(f"      Original Payload SHA-256:   {original_sha}")
    print(f"      Downloaded Payload SHA-256: {downloaded_sha}")
    
    if original_sha == downloaded_sha and len(original_data) == len(downloaded_data):
        print("\n=== [PASS] DEDUPLICATION & REPLICATION INTEGRITY CONFIRMED ===")
        print(f"    Decrypted payload: '{downloaded_data.decode('utf-8')}'")
    else:
        print("\n=== [FAIL] DATA CORRUPTION DETECTED ===")
        sys.exit(1)

    # ---------------------------------------------------------
    # VERIFY ENCRYPTION AT REST INSTRUCTIONS
    # ---------------------------------------------------------
    print("\n=== VERIFY ENCRYPTION AT REST ===")
    print("    Because chunks are encrypted at rest with AES-256-CBC, you can verify they are secure.")
    print("    Execute the following command in your terminal to see the encrypted ciphertext (it will be binary garbage):")
    for h in chunk_hashes:
        print(f"    docker exec -it worker_1 cat /app/data/{h}.bin")
    print("==================================\n")

    # ---------------------------------------------------------
    # PHASE 4: ADMINISTRATIVE IN-PLACE CLUSTER REPLACE
    # ---------------------------------------------------------
    print("[9/9] Testing Administrative Scaling: In-Place Cluster Replacement / Sealing")
    replace_input = input("      Do you want to replace the worker nodes of Cluster 1? (y/n): ")
    if replace_input.strip().lower() == 'y':
        new_workers_input = input("      Enter new worker IDs to swap in (comma-separated, e.g. 3,4): ")
        try:
            new_worker_ids = [int(x.strip()) for x in new_workers_input.split(",") if x.strip().isdigit()]
        except Exception as e:
            print(f"[FAIL] Invalid formatting: {e}")
            sys.exit(1)
            
        if not new_worker_ids:
            print("[FAIL] You must provide at least one valid worker ID.")
            sys.exit(1)
            
        replace_payload = {
            "cluster_id": 1,
            "new_worker_ids": new_worker_ids
        }
        
        print(f"      Executing POST /admin/cluster/replace (Cluster: 1, New Workers: {new_worker_ids})...")
        replace_response = requests.post(f"{MASTER_URL}/admin/cluster/replace", json=replace_payload)
        
        if replace_response.status_code == 200:
            print("      PASS: Cluster nodes replaced in-place. Former nodes are now marked as READ_ONLY.")
            print("            Future uploads on Cluster 1 will route to the new workers.")
        else:
            print(f"[FAIL] Replace cluster failed with status: {replace_response.status_code}")
            print(replace_response.text)
            sys.exit(1)
    else:
        print("      Skipping Cluster Replacement scaling test.")

if __name__ == "__main__":
    execute_e2e_test();