"""The data plane: a user-run shim that is the ONLY holder of broker
credentials. It serves MCP on localhost to the agent, dials brokers with the
existing gateway runtime under its own root, and treats the platform as the
policy/approval authority, never as a credential store."""
