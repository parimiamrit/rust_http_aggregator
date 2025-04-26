
# Rust HTTP Aggregator

A Rust application designed to make multiple HTTP requests in parallel.

## Overview

`rust_http_aggregator` is a tool built with Rust that allows users to send multiple HTTP requests concurrently.  
This can be particularly useful for aggregating data from various APIs or endpoints efficiently.

## Features

- Parallel HTTP requests  
- Built with Rust for performance and safety  
- Dockerfile included for containerization

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) installed on your machine  
- [Docker](https://www.docker.com/get-started) (optional, for containerization)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/parimiamrit/rust_http_aggregator.git
   cd rust_http_aggregator
   ```

2. Build the project:

   ```bash
   cargo build --release
   ```

3. Run the application:

   ```bash
   cargo run
   ```

### Docker

To build and run the application using Docker:

1. Build the Docker image:

   ```bash
   docker build -t rust_http_aggregator .
   ```

2. Run the Docker container:

   ```bash
   docker run rust_http_aggregator
   ```

