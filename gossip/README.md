
# Week 1 – System Load Collection and Gossip Message Creation

## Overview
Week 1 focuses on collecting system resource information and creating Gossip messages for communication between MeshWeaver nodes.

## Files and Description

- **collector.py:** Collects CPU usage, total RAM, and free RAM using the `psutil` library.
- **protocol.py:** Defines the Gossip message structure and provides functionality to create messages containing node load information.

## Key Features

- Monitors CPU usage.
- Collects total and available RAM information.
- Creates Gossip messages containing node ID, CPU usage, free RAM, and timestamp.
- Connects collected system information with the Gossip message structure.

## Testing

Tests cover system load collection and Gossip message creation, including validation of the collected values and message fields.

## Conclusion

Week 1 establishes the basic system load monitoring and Gossip message creation functionality required for communication between MeshWeaver nodes.
