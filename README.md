# 🏔️ Whistler Snow Sync

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Notion](https://img.shields.io/badge/Notion-API-black.svg?logo=notion)
![Gemini AI](https://img.shields.io/badge/Google%20Gemini-AI%20Vision-8E75B2.svg)
![Playwright](https://img.shields.io/badge/Playwright-Scraping-green.svg)

**Whistler Snow Sync** is an intelligent, automated data pipeline that scrapes real-time weather, lift status, and historical snowfall data for Whistler Blackcomb and synchronizes it into a Notion dashboard.

It utilizes **Google Gemini AI** to visually analyze webcam feeds (determining if it's "Bluebird" or "Foggy") and features an adaptive scheduler that adjusts update frequencies based on forecast release times.

---

## ⚡ Features

* **☁️ Multi-Elevation Forecasting:** Aggregates weather reports for **1480m**, **1800m**, and **2248m** from multiple sources (Snow-Forecast.com & RWDI).
* **📸 AI Webcam Analysis:** Uses Google Gemini 1.5 Flash to look at webcam images and classify sky conditions (e.g., *Bluebird, Overcast, Night*).
* **🚠 Lift Status Sync:** Scrapes real-time open/close status and elevation data for all lifts.
* **📅 Historical Archiving:** incremental scraping of daily snowfall history to build a long-term climate dataset.
* **🛡️ Smart Deduplication:** Robust logic to prevent duplicate entries in Notion, ensuring clean data history.
* **🔄 Adaptive Service:** A daemon service (`service.py`) that runs continuously and recovers automatically from crashes.

---

## 🏗️ Architecture

```mermaid
graph TD
    A[WhistlerWebsites] -->|Scrape HTML| B(Playwright Core)
    C[Webcams] -->|Images| B
    B --> D{Job Controller}
    
    D -->|Raw Text| E[Weather Parser]
    D -->|Images| F[Gemini AI Vision]
    
    F -->|Condition: 'Bluebird'| G[Data Aggregator]
    E --> G
    
    G -->|Cleaned Data| H[Notion Client]
    H -->|Upsert Rows| I[(Notion Database)]