class SessionState:
    def __init__(self):
        # Each session_id maps to a dict of session values
        self.sessions = {}

    def set_value(self, session_id, key, value):
        if session_id not in self.sessions:
            self.sessions[session_id] = {}
        self.sessions[session_id][key] = value

    def get_value(self, session_id, key, default=None):
        return self.sessions.get(session_id, {}).get(key, default)

    def get_session(self, session_id):
        return self.sessions.get(session_id, {})

    def reset(self):
        self.sessions = {}

session_state = SessionState()