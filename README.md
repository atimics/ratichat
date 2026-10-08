# 🤖 Chatbot System

For Discord community chat, follow the [Discord setup guide](docs/DISCORD_SETUP.md).

A sophisticated, context-aware chatbot system with dynamic tool-based architecture, multi-platform integration, and advanced conversation state management. Built for intelligent conversations across Matrix and Farcaster platforms with comprehensive AI-driven decision making.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency%20management-poetry-blue)](https://python-poetry.org/)
[![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)
[![Code Quality](https://img.shields.io/badge/code%20quality-black%20%7C%20flake8%20%7C%20mypy-blue)](pyproject.toml)

## 📋 Table of Contents

- [🌟 Key Features](#-key-features)
- [🏗️ Architecture Overview](#️-architecture-overview)
- [🚀 Quick Start](#-quick-start)
- [💻 Local Development](#-local-development)
- [🌐 World State Management](#-world-state-management)
- [🔧 Configuration](#-configuration)
- [🚀 Deployment](#-deployment)
- [🧪 Testing](#-testing)
- [📖 Documentation](#-documentation)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

## 🌟 Key Features

- **🔧 Dynamic Tool Architecture**: Extensible tool system with runtime registration and AI integration
- **🧠 Context-Aware Conversations**: Maintains evolving world state across conversations with advanced deduplication
- **🌐 Multi-Platform Integration**: Support for Matrix and Farcaster with standardized tool interfaces
- **👁️ AI Conversation Continuity**: Bot tracks its own messages for improved conversation flow
- **💾 Persistent State Management**: Robust storage of conversation context and world state
- **🤖 AI-Powered Decision Making**: Intelligent response generation with dynamic tool awareness
- **📊 Advanced Rate Limiting**: Smart rate limiting with backoff and quota management
- **🔄 Thread Management**: Intelligent conversation thread tracking and context preservation
- **📧 Matrix Room Management**: Auto-join functionality with invite handling
- **📱 Enhanced User Profiling**: Rich user metadata tracking for social platforms

## 🏗️ Architecture Overview

The system has been architected with a dynamic tool-based design for maximum extensibility and maintainability. The architecture follows a layered approach with clear separation of concerns.

### 🔧 Core Components

#### **ToolRegistry & Tool System**
- **ToolRegistry**: Manages dynamic tool registration and provides AI-ready descriptions
- **ToolInterface**: Abstract base class for all tools with standardized execution patterns
- **ActionContext**: Comprehensive dependency injection for tools (observers, configurations, state managers)

#### **World State Management**
- **WorldStateManager**: Central state coordinator with advanced message deduplication
- **Message & Channel Models**: Rich data models supporting multi-platform message metadata
- **Thread Management**: Intelligent conversation threading for platforms like Farcaster
- **Rate Limiting**: Built-in rate limit tracking and enforcement

#### **AI Integration & Orchestration**
- **ContextAwareOrchestrator**: Main coordinator using the tool system with intelligent cycle management
- **AIDecisionEngine**: Updated to receive dynamic tool descriptions and optimized payloads
- **Context Manager**: Advanced conversation context preservation and retrieval

#### **Platform Integrations**
- **Matrix Integration**: Full Matrix protocol support with room management and invite handling
- **Farcaster Integration**: Complete Farcaster API integration with enhanced user profiling
- **Standardized Interfaces**: Unified message and action handling across platforms

### 🛠️ Tool System

All platform interactions are handled through standardized tools with consistent interfaces:

#### **Core Tools**
- `WaitTool` - Intelligent observation and waiting actions with configurable intervals
- `ObserveTool` - Advanced world state observation with filtering and summarization

#### **Matrix Tools**
- `SendMatrixReplyTool` - Matrix reply functionality with thread context awareness
- `SendMatrixMessageTool` - Matrix message sending with formatting support
- `JoinMatrixRoomTool` - Automated room joining with invite acceptance

#### **Farcaster Tools**
- `SendFarcasterPostTool` - Farcaster posting with media support and rate limiting
- `SendFarcasterReplyTool` - Farcaster replying with thread context preservation
- `LikeFarcasterPostTool` - Social engagement actions with deduplication
- `QuoteFarcasterPostTool` - Quote casting with content attribution
- `FollowFarcasterUserTool` - User following functionality
- `DeleteFarcasterPostTool` - Delete your own Farcaster posts/casts
- `DeleteFarcasterReactionTool` - Remove likes/recasts from posts
- `SendFarcasterDirectMessageTool` - Private messaging capabilities

### 🎯 Key Benefits

- **🔄 Extensibility**: Add new tools by implementing `ToolInterface` and registering
- **🧹 Maintainability**: Platform logic isolated in dedicated tool classes with clear boundaries
- **🧪 Testability**: Clean dependency injection via `ActionContext` enables comprehensive testing
- **🤖 AI Integration**: Tool descriptions automatically update AI capabilities and decision-making
- **📏 Consistency**: Standardized parameter schemas and error handling across all tools
- **⚡ Performance**: Optimized payload generation and intelligent message filtering
- **🔒 Reliability**: Robust error handling, rate limiting, and state consistency

## 🚀 Quick Start

### 🐳 Docker Deployment (Recommended)

The fastest way to get started is using Docker:

```bash
# 1. Clone the repository
git clone <repository-url>
cd python3-poetry-pyenv

# 2. Setup environment
cp .env.example .env
nano .env  # Fill in your API keys and credentials

# 3. Deploy with Docker
./scripts/deploy.sh

# 4. Monitor logs
docker-compose logs -f chatbot
```

### 🛠️ Development Setup

For local development and testing:

```bash
# 1. Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# 2. Install dependencies
poetry install

# 3. Configure environment
cp .env.example .env
nano .env  # Add your credentials

# 4. Run the system
poetry run python -m chatbot.main
```

### 🎮 Available Tasks

Use VS Code tasks or run them directly:

```bash
# Run the main chatbot
poetry run python -m chatbot.main

# Run with management UI
poetry run python -m chatbot.main_with_ui

# Run control panel
poetry run python control_panel.py

# Run tests
poetry run pytest tests/ -v

# Format code
poetry run black chatbot/ && poetry run isort chatbot/

# Lint code
poetry run flake8 chatbot/ && poetry run mypy chatbot/
```

## 💻 Local Development

### 🏠 Migrating from GitHub Codespaces

If you're currently using GitHub Codespaces and want to migrate to local development for better performance and no resource limits:

#### Quick Migration
```bash
# 1. Clone to your local machine
git clone <your-repo-url>
cd matrixbot

# 2. Run the migration script
./migrate_to_local.sh

# 3. Open in VS Code and reopen in container
code .
# When prompted: "Reopen in Container"
```

#### Manual Setup
1. **Install Prerequisites**:
   - [Docker Desktop](https://www.docker.com/products/docker-desktop/)
   - [Visual Studio Code](https://code.visualstudio.com/)
   - [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

2. **Setup Environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your actual values
   ```

3. **Open in Dev Container**:
   - Open project in VS Code
   - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS)
   - Select "Dev Containers: Reopen in Container"

### 🔧 Local Development Benefits

- **Performance**: Faster file I/O and build times
- **Resources**: Use your full machine resources
- **Persistence**: Data persists between sessions
- **Offline**: Work without internet connection
- **Debugging**: Better debugging experience

### 📂 Development Workflow

```bash
# Start all services
docker-compose up -d

# Run the chatbot in development
poetry run python run.py

# Run with UI
poetry run python chatbot/main_with_ui.py

# Run tests
poetry run pytest

# View logs
docker-compose logs -f chatbot_backend
```

For detailed local development setup, see [LOCAL_DEVELOPMENT.md](./LOCAL_DEVELOPMENT.md).

## 🌐 World State Management

The system implements a sophisticated world state management approach that maintains comprehensive awareness of all platform activities and conversations.

### 📊 State Components

#### **Message Management**
- **Multi-Platform Messages**: Unified message model supporting Matrix and Farcaster with platform-specific metadata
- **Rich User Profiles**: Enhanced user information including follower counts, bios, profile pictures, and verification badges
- **Deduplication**: Advanced message deduplication across channels and platforms to prevent processing duplicates
- **Thread Tracking**: Intelligent conversation thread management for platforms supporting threaded discussions

#### **Channel & Room Tracking**
- **Dynamic Channel Creation**: Automatic channel discovery and registration as the bot encounters new rooms/feeds
- **Activity Summarization**: Real-time activity summaries with user engagement metrics and timestamp ranges
- **Matrix Room Metadata**: Complete room information including topics, member counts, power levels, and encryption status
- **Invite Management**: Pending Matrix room invites with automated acceptance workflows

#### **Action History**
- **Comprehensive Logging**: Complete audit trail of all bot actions with parameters and results
- **Action Deduplication**: Prevents duplicate actions (likes, replies, follows) with intelligent tracking
- **Scheduled Action Updates**: Support for updating scheduled/pending actions with final results
- **Rate Limit Integration**: Action history informs rate limiting decisions and backoff strategies

### 🔄 State Optimization

#### **AI Payload Optimization**
- **Primary Channel Focus**: Detailed information for the active conversation channel
- **Smart Summarization**: Intelligent summarization of secondary channels to reduce token usage
- **User Context Filtering**: Bot's own messages are filtered out to focus on external interactions
- **Configurable Truncation**: Adjustable limits for messages, actions, and thread history based on AI model constraints

#### **Performance Features**
- **Efficient Updates**: Incremental state updates with minimal memory footprint
- **Background Processing**: Non-blocking state updates that don't interrupt conversation flow
- **Smart Caching**: Intelligent caching of frequently accessed state components
- **Memory Management**: Automatic cleanup of old messages and actions to prevent memory bloat

### 📈 State Analytics

The world state provides rich analytics for understanding conversation patterns and bot performance:

- **Conversation Metrics**: Message frequency, user engagement, and response patterns
- **Platform Activity**: Cross-platform activity correlation and user behavior analysis
- **Bot Performance**: Action success rates, response times, and error patterns
- **Social Dynamics**: User interaction patterns, thread participation, and engagement quality

## 🔧 Configuration

The system uses environment variables for configuration. Copy `.env.example` to `.env` and configure:

### � Required Settings

```bash
# AI Configuration
AI_MODEL=openai/gpt-4o-mini
OPENROUTER_API_KEY=your_openrouter_key_here

# Matrix Configuration
MATRIX_HOMESERVER=https://matrix.example.org
MATRIX_USER_ID=@your-bot:example.org
MATRIX_PASSWORD=your_secure_password
MATRIX_ROOM_ID=!yourRoom:example.org
```

### ⚙️ Optional Settings

```bash
# Farcaster Integration
NEYNAR_API_KEY=your_neynar_key
FARCASTER_BOT_FID=your_bot_fid
FARCASTER_BOT_USERNAME=your_bot_username

# Performance Tuning
OBSERVATION_INTERVAL=2.0
MAX_CYCLES_PER_HOUR=300
AI_CONVERSATION_HISTORY_LENGTH=10

# Alternative LLM Provider
PRIMARY_LLM_PROVIDER=ollama  # or "openrouter"
OLLAMA_API_URL=http://localhost:11434
```

See [Configuration Guide](DEVELOPMENT.md#configuration) for detailed options.

## 🚀 Deployment

### 🐳 Docker Production Deployment

1. **Configure production environment**:
```bash
cp .env.example .env.production
# Edit .env.production with production credentials
```

2. **Deploy using Docker Compose**:
```bash
docker-compose up -d
```

3. **Monitor and manage**:
```bash
# View logs
docker-compose logs -f chatbot

# Restart services
docker-compose restart

# Update deployment
git pull
docker-compose build
docker-compose up -d
```

### 🖥️ Traditional Server Deployment

```bash
# Install system dependencies
sudo apt update
sudo apt install python3.10 python3-pip

# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Deploy application
git clone <repository-url>
cd python3-poetry-pyenv
poetry install --only=main
cp .env.example .env
# Edit .env with production settings

# Run as service (using systemd)
sudo cp scripts/chatbot.service /etc/systemd/system/
sudo systemctl enable chatbot
sudo systemctl start chatbot
```

### ☁️ Cloud Deployment

The system supports deployment on:
- **AWS ECS**: Container-based deployment
- **Google Cloud Run**: Serverless container deployment  
- **Azure Container Instances**: Simple container deployment
- **DigitalOcean App Platform**: Managed deployment

See [Deployment Guide](DEVELOPMENT.md#deployment-options) for detailed instructions.

For the planned `rati.chat` service, see the [Fly.io deployment
options](docs/DEPLOYMENT_OPTIONS.md) and the [cost and usage
ledger](docs/COST_AND_USAGE_LEDGER.md). The invited pilot uses Continuwuity,
Pocket ID passkeys, Element Web, and a fixed-cost target below $25 per month.

## 🧪 Testing

The system includes comprehensive testing infrastructure:

```bash
# Run all tests
poetry run pytest tests/ -v

# Run with coverage
poetry run pytest tests/ --cov=chatbot --cov-report=html --cov-report=term

# Run specific test categories
poetry run pytest tests/test_core.py -v
poetry run pytest tests/test_world_state_comprehensive.py -v

# Performance testing
poetry run pytest tests/ -m "not slow"  # Skip slow tests
poetry run pytest tests/ -m "slow"      # Run only slow tests
```

### 🏗️ Test Structure

- **Unit Tests**: Individual component testing
- **Integration Tests**: Multi-component interactions  
- **End-to-End Tests**: Complete workflow testing
- **Performance Tests**: Load and stress testing

See [Testing Guide](DEVELOPMENT.md#testing-guidelines) for detailed information.
tests/
├── test_ai_engine.py                 # AI decision engine testing
├── test_core.py                      # Core component unit tests
├── test_orchestrator_extended.py     # Orchestrator integration tests
├── test_world_state_extended.py      # World state management tests
├── test_tool_system.py              # Tool registry and execution tests
├── test_matrix_tools_and_observer.py # Matrix platform integration
├── test_farcaster_tools_follow_dm.py # Farcaster platform features
├── test_integration.py              # Full system integration tests
└── test_robust_json_parsing.py      # AI response parsing reliability
```

### 🎯 Quality Metrics

#### **Code Quality Tools**
- **Black**: Consistent code formatting across the entire codebase
- **isort**: Import statement organization and optimization
- **flake8**: Code style enforcement and basic linting
- **mypy**: Static type checking for improved reliability
- **pytest**: Comprehensive test framework with async support

#### **Coverage Reporting**
```bash
# Run tests with coverage
poetry run pytest tests/ --cov=chatbot --cov-report=html --cov-report=term

# View HTML coverage report
open htmlcov/index.html
```

### 🔧 Development Workflow

#### **Available Tasks**
```bash
# Main application
poetry run python -m chatbot.main

# Testing
poetry run pytest tests/ -v                           # Run all tests
poetry run pytest tests/ --cov=chatbot               # With coverage

# Code Quality
poetry run black chatbot/ && poetry run isort chatbot/  # Format code
poetry run flake8 chatbot/ && poetry run mypy chatbot/  # Lint and type check

# Development Tools
poetry run python control_panel.py                    # Control panel interface
```

#### **VS Code Tasks**
The project includes pre-configured VS Code tasks for common operations:
- **Run Chatbot Main Application**: Starts the main bot with background execution
- **Run Control Panel**: Launches the web-based control interface
- **Run Tests**: Executes the full test suite
- **Run Tests with Coverage**: Tests with HTML coverage reporting
- **Format Code**: Applies Black and isort formatting
- **Lint Code**: Runs flake8 and mypy validation

## 🐛 Debugging & Troubleshooting

### 📋 Common Issues

#### **Connection Problems**
```bash
# Check Matrix connectivity
grep "matrix_connected" chatbot.log

# Verify Farcaster API access
grep "farcaster_connected" chatbot.log

# Monitor rate limiting
grep "rate_limit" chatbot.log
```

#### **State Management Issues**
```bash
# Check world state consistency
grep "WorldState:" chatbot.log

# Monitor message deduplication
grep "Deduplicated message" chatbot.log

# Track action execution
grep "Action completed" chatbot.log
```

### 🔍 Debug Tools

#### **Enhanced Logging**
```bash
# Set debug level logging
export LOG_LEVEL=DEBUG

# Monitor specific components
grep "ContextAwareOrchestrator" chatbot.log
grep "ToolRegistry" chatbot.log
grep "WorldStateManager" chatbot.log
```

#### **Control Panel Interface**
The system includes a web-based control panel for real-time monitoring:
```bash
poetry run python control_panel.py
# Access at http://localhost:5000
```

Features:
- **Real-time State Monitoring**: Live view of world state and recent activities
- **Action History**: Complete audit trail of bot actions and results
- **Platform Status**: Connection status and health metrics for all platforms
- **Configuration Viewer**: Current configuration settings and environment variables

## 📖 Documentation

Comprehensive documentation is available:

- **[README.md](README.md)**: This file - quick start and overview
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Detailed system architecture and design
- **[DEVELOPMENT.md](DEVELOPMENT.md)**: Development setup, workflows, and guidelines
- **[API.md](API.md)**: API reference and integration details
- **[Deployment options](docs/DEPLOYMENT_OPTIONS.md)**: Fly.io choices, costs, recovery targets, and upgrade rules
- **[Cost and usage ledger](docs/COST_AND_USAGE_LEDGER.md)**: Monthly cost attribution and uptake measures

### 📚 Additional Resources

- **Tool System**: See `chatbot/tools/` for individual tool implementations
- **Configuration**: Review `.env.example` for all available settings
- **Scripts**: Check `scripts/` directory for deployment and utility scripts

## 🤝 Contributing

We welcome contributions! Please see [DEVELOPMENT.md](DEVELOPMENT.md) for:

- Development environment setup
- Code quality standards
- Testing requirements
- Pull request process

### 🛠️ Development Commands

```bash
# Setup development environment
poetry install
poetry run pre-commit install

# Code quality checks
poetry run black chatbot/           # Format code
poetry run isort chatbot/           # Sort imports  
poetry run flake8 chatbot/          # Lint code
poetry run mypy chatbot/            # Type checking

# Run tests
poetry run pytest tests/ -v        # All tests
poetry run pytest tests/ --cov     # With coverage
```

## 📄 License

This project is licensed under the Creative Commons Attribution-NonCommercial 4.0 International License. See [LICENSE](LICENSE) file for details.

**Key Terms:**
- ✅ **Share & Adapt**: You can copy, redistribute, remix, and build upon the material
- 🏷️ **Attribution Required**: You must give appropriate credit and indicate changes
- 🚫 **Non-Commercial**: Commercial use requires explicit written permission
- 📧 **Commercial Licensing**: Contact us for commercial use permissions

For commercial use, licensing, or any revenue-generating applications, please obtain written permission from the copyright holder.

## 🆘 Support

- **Issues**: [GitHub Issues](../../issues)
- **Discussions**: [GitHub Discussions](../../discussions)
- **Documentation**: [Project Documentation](DEVELOPMENT.md)

---

<div align="center">

**[⬆ Back to Top](#-chatbot-system)**

Built with ❤️ using Python, Poetry, and modern async technologies

</div>
