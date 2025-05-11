CREATE TABLE user_permissions (
    permission_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    permission_type TEXT CHECK (permission_type IN ('read', 'write', 'execute')) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, folder_path),
    CONSTRAINT check_permission_type CHECK (permission_type IN ('read', 'write', 'execute'))
);
