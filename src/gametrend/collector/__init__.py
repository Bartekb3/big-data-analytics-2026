"""Raw data collector: polls the Roblox and Steam APIs and archives every HTTP response unchanged.

A supporting tool, not part of the Lambda pipeline: it gathers real data (with original
timestamps) that replay producers later stream into Kafka. Design: architecture/collector/README.md.
"""
