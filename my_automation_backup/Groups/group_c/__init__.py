def __init__(self):
    self.config_loader = None
    if config_loader_available and get_config_loader:
        try:
            self.config_loader = get_config_loader()
            print("[Engine] Config loader connected")
        except Exception as e:
            print(f"[Engine] Config loader init error: {e}")
    else:
        print("[Engine] Config loader not available - using defaults")
    
    print("[Engine] Initialized")