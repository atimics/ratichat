"""
Centralized Configuration Management

This module provides centralized configuration management using Pydantic BaseSettings
to load and validate all configuration from environment variables and .env files.
"""

import json
import os
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


def load_config_json() -> dict:
    """Load configuration from config.json file if it exists."""
    config_path = Path("data/config.json")
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                # Remove metadata fields that aren't configuration
                config.pop("_setup_completed", None)
                config.pop("_setup_timestamp", None)
                return config
        except Exception as e:
            print(f"Warning: Error reading config.json: {e}")
    return {}


class AppConfig(BaseSettings):
    """Centralized application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Chatbot Core
    BOT_PUBLIC_URL: str = ""
    CHATBOT_DB_PATH: str = "data/chatbot.db"
    CHATBOT_ENV: str = "development"
    INTEGRATION_CREDENTIAL_KEY: Optional[str] = None
    OBSERVATION_INTERVAL: float = 2.0
    MAX_CYCLES_PER_HOUR: int = 300
    MAX_ACTIONS_PER_HOUR: int = 600
    AI_MODEL: str = "openai/gpt-4o-mini"
    AI_MULTIMODAL_MODEL: str = "openai/gpt-4o"  # Model for image/video analysis
    OPENROUTER_API_KEY: Optional[str] = None  # Made optional for demo mode
    BOT_CAPABILITY_PROFILE: str = "public"
    LOG_LEVEL: str = "INFO"
    
    # Web Search and Research
    WEB_SEARCH_MODEL: str = "openai/gpt-4o-mini"  # OpenRouter model with the web search server tool

    # Matrix (Optional since we removed Synapse dependency)
    MATRIX_HOMESERVER: Optional[str] = None
    MATRIX_USER_ID: Optional[str] = None
    MATRIX_PASSWORD: Optional[str] = None
    MATRIX_ACCESS_TOKEN: Optional[str] = None
    MATRIX_ROOM_ID: str = (
        "#robot-laboratory:chat.ratimics.com"  # Default initial room to monitor
    )
    PUBLIC_MATRIX_ROOM_IDS: str = ""
    MATRIX_DEVICE_ID: Optional[str] = None
    MATRIX_MEDIA_GALLERY_ROOM_ID: Optional[str] = None  # Dedicated channel for auto-posting generated media
    MATRIX_CONTROL_ROOM_ID: str = ""
    MATRIX_OPERATOR_USER_IDS: str = ""
    MATRIX_MANAGED_ROOM_IDS: str = ""
    MATRIX_ADMIN_ROOM_ID: str = ""
    MATRIX_ADMIN_ACCESS_TOKEN: Optional[str] = None
    MATRIX_ADMIN_SERVER_USER_ID: str = "@conduit:rati.chat"
    MATRIX_BACKUP_INTERVAL_SECONDS: int = 86400

    # Discord: comma-separated server and text channel IDs.
    DISCORD_BOT_TOKEN: Optional[str] = None
    DISCORD_ALLOWED_GUILD_IDS: str = ""
    DISCORD_ALLOWED_CHANNEL_IDS: str = ""
    DISCORD_MESSAGE_RATE_LIMIT_PER_MINUTE: int = 10
    DISCORD_MAX_MESSAGE_CHARS: int = 4000
    DISCORD_OWNER_USER_IDS: str = ""
    SHARED_AWARENESS_ENABLED: bool = True
    TASK_MODEL_ROUTING_ENABLED: bool = True
    TASK_BUDGET_USD: float = 0.10
    DISCORD_MESSAGE_CONTENT_ENABLED: bool = False
    CONVERSATION_PARTICIPATION_ENABLED: bool = True
    RESEARCH_RETENTION_DAYS: int = 7
    RESEARCH_SOURCE_TTL_SECONDS: int = 300
    SOURCE_WATCH_DAILY_LOOKUP_BUDGET: int = 24
    LIVE_MONITOR_DAILY_LOOKUP_BUDGET: int = 7200
    TRONGRID_API_KEY: Optional[str] = None
    BLOCKSCOUT_API_KEY: Optional[str] = None
    PROACTIVE_DISCORD_CHANNEL_IDS: str = ""
    PROACTIVE_PUBLIC_SOURCES_ENABLED: bool = False
    PROACTIVE_TIMEZONE: str = "America/Vancouver"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_ALLOWED_CHAT_IDS: str = ""
    TELEGRAM_ALLOWED_SENDER_IDS: str = ""
    TELEGRAM_MESSAGE_RATE_LIMIT_PER_MINUTE: int = 10
    TELEGRAM_MAX_MESSAGE_CHARS: int = 4000
    TELEGRAM_OFFSET_PATH: str = "data/telegram_offset.txt"
    TELEGRAM_OPERATOR_QUEUE_PATH: str = "data/telegram_operator_queue.jsonl"
    TELEGRAM_OPERATOR_QUEUE_MAX_BYTES: int = 1_048_576

    # Local management API. The token is required for every /api request other
    # than health checks and for the log WebSocket.
    ADMIN_API_TOKEN: Optional[str] = None
    ADMIN_API_HOST: str = "127.0.0.1"
    ADMIN_API_PORT: int = 8000
    ADMIN_API_ALLOWED_ORIGINS: str = (
        "http://127.0.0.1:8000,http://localhost:8000,http://localhost:3000"
    )
    INTEGRATION_CONNECT_TIMEOUT_SECONDS: float = 15.0
    DEVICE_NAME: str = "ratichat_bot"

    # Farcaster (Optional)
    NEYNAR_API_KEY: Optional[str] = None
    FARCASTER_BOT_FID: Optional[str] = None
    FARCASTER_BOT_SIGNER_UUID: Optional[str] = None
    FARCASTER_BOT_USERNAME: Optional[str] = None  # Bot's username for filtering
    FARCASTER_WEBHOOK_SECRET: Optional[str] = None

    # Ecosystem Token Tracking
    ECOSYSTEM_TOKEN_CONTRACT_ADDRESS: Optional[str] = "Ci6Y1UX8bY4jxn6YiogJmdCxFEu2jmZhCcG65PStpump"  # Contract address of the token
    ECOSYSTEM_TOKEN_NETWORK: str = "solana"  # Network of the token (ethereum, optimism, base, arbitrum, solana)
    NUM_TOP_HOLDERS_TO_TRACK: int = 10  # Number of top holders to monitor
    TOP_HOLDERS_UPDATE_INTERVAL_MINUTES: int = 60  # How often to refresh the top holders list
    HOLDER_CAST_HISTORY_LENGTH: int = 5  # Number of recent casts to store per holder

    # OpenRouter specific from original .env.example (these might be for other services or documentation)
    YOUR_SITE_URL: Optional[str] = None
    YOUR_SITE_NAME: Optional[str] = None

    # Ollama (Optional - from .env.example)
    PRIMARY_LLM_PROVIDER: str = "openrouter"
    OLLAMA_API_URL: Optional[str] = "http://localhost:11434"
    OLLAMA_DEFAULT_CHAT_MODEL: Optional[str] = "llama3"
    OLLAMA_DEFAULT_SUMMARY_MODEL: Optional[str] = "llama3"
    # AI payload truncation settings - optimized to prevent 413 Payload Too Large errors
    AI_CONVERSATION_HISTORY_LENGTH: int = 7  # Max messages per channel for AI payload (reduced from 10)
    AI_ACTION_HISTORY_LENGTH: int = 3  # Max actions in history for AI payload (reduced from 5)
    AI_THREAD_HISTORY_LENGTH: int = 3  # Max thread messages for AI payload (reduced from 5)
    AI_OTHER_CHANNELS_SUMMARY_COUNT: int = (
        2  # How many other active channels to summarize (reduced from 3)
    )
    AI_OTHER_CHANNELS_MESSAGE_SNIPPET_LENGTH: int = (
        75  # Length of snippet for other channels
    )
    AI_INCLUDE_DETAILED_USER_INFO: bool = (
        False  # Include full user metadata or summarize - False reduces payload size significantly
    )

    # v0.0.3: Media Generation & Permaweb Storage Configuration

    # Replicate Configuration
    REPLICATE_API_TOKEN: Optional[str] = None
    REPLICATE_IMAGE_MODEL: str = "stability-ai/sdxl"
    REPLICATE_LORA_WEIGHTS_URL: Optional[str] = None
    REPLICATE_LORA_SCALE: Optional[float] = 0.75

    # Google AI Media Generation
    GOOGLE_API_KEY: Optional[
        str
    ] = None  # For Google AI services (separate from OpenRouter)
    GOOGLE_GEMINI_IMAGE_MODEL: str = "gemini-1.5-flash-latest"
    GOOGLE_VEO_VIDEO_MODEL: str = "models/veo-experimental-v1"

    # Arweave Configuration (Internal Uploader Service)
    ARWEAVE_INTERNAL_UPLOADER_SERVICE_URL: str = "http://arweave-uploader:8001"
    ARWEAVE_GATEWAY_URL: str = "https://arweave.net"
    ARWEAVE_UPLOADER_API_KEY: Optional[str] = None

    # NFT & Airdrop Configuration (v0.0.4)
    NFT_DEV_WALLET_PRIVATE_KEY: Optional[str] = None  # For sponsoring transactions or direct minting
    BASE_RPC_URL: Optional[str] = None  # e.g., from Alchemy or Infura
    NFT_COLLECTION_NAME: str = "AI Collective"
    NFT_COLLECTION_SYMBOL: str = "AIC"
    NFT_COLLECTION_ADDRESS_BASE: Optional[str] = None  # The address of your NFT contract on Base
    NFT_METADATA_UPLOAD_SERVICE: str = "arweave"  # or "ipfs"
    
    # Frame Server Configuration
    FRAMES_BASE_URL: Optional[str] = None  # Base URL for serving frames (e.g., https://yourbot.com)
    FRAMES_WEBHOOK_SECRET: Optional[str] = None  # Secret for validating frame requests
    
    # Airdrop Eligibility Criteria
    AIRDROP_MIN_ECOSYSTEM_TOKEN_BALANCE_SOL: float = 1000.0
    AIRDROP_MIN_ECOSYSTEM_NFT_COUNT_BASE: int = 1
    AIRDROP_ELIGIBILITY_CHECK_INTERVAL_HOURS: int = 6  # How often to check eligibility

    # Tool Cooldowns & Resource Limits
    IMAGE_GENERATION_COOLDOWN_SECONDS: int = 120  # 2 minutes
    VIDEO_GENERATION_COOLDOWN_SECONDS: int = 600  # 10 minutes
    STORE_MEMORY_COOLDOWN_SECONDS: int = 60  # 1 minute
    MAX_IMAGE_GENERATIONS_PER_HOUR: int = 15
    MAX_VIDEO_GENERATIONS_PER_HOUR: int = 5
    MAX_MEMORIES_STORED_PER_HOUR: int = 30

    # Popular Media Archival
    POPULAR_MEDIA_ARCHIVAL_THRESHOLD_LIKES: int = 5
    POPULAR_MEDIA_ARCHIVAL_INTERVAL_MINUTES: int = 30

    # JSON Observer and Interactive Executor Configuration
    MAX_EXPANDED_NODES: int = 8  # Maximum number of simultaneously expanded nodes
    DEFAULT_PINNED_NODES: list[str] = [
        "channels.matrix.primary",  # Primary Matrix channel
        "system.notifications",     # Global notifications
        "system.rate_limits"       # Rate limit status
    ]
    AI_SUMMARY_MODEL: str = "openai/gpt-4o-mini"  # Model for generating node summaries
    ENABLE_TWO_PHASE_AI_PROCESS: bool = False  # Enable separate exploration/action phases
    MAX_EXPLORATION_ROUNDS: int = 3  # Max rounds in exploration phase if two-phase enabled
    
    # GitHub ACE (Autonomous Code Evolution) Integration
    GITHUB_TOKEN: Optional[str] = None
    GITHUB_USERNAME: Optional[str] = None


def matrix_auth_is_configured(config: AppConfig) -> bool:
    """Return whether Matrix has a complete password or access-token login."""
    if not config.MATRIX_HOMESERVER or not config.MATRIX_USER_ID:
        return False
    if config.MATRIX_ACCESS_TOKEN:
        return bool(config.MATRIX_DEVICE_ID)
    return bool(config.MATRIX_PASSWORD)


# Global settings instance
def create_settings() -> AppConfig:
    """Create settings instance with merged configuration from env and config.json."""
    # Load from config.json first
    json_config = load_config_json()
    
    # Set environment variables from config.json (they will override only if not already set)
    for key, value in json_config.items():
        if key not in os.environ:
            os.environ[key] = str(value)
    
    return AppConfig()


settings = create_settings()
