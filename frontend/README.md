# DataForge Frontend

Next.js 14 frontend for the DataForge data preparation platform.

## Tech Stack

- **Next.js 14** with App Router
- **Tailwind CSS** for styling
- **shadcn/ui** component library
- **TanStack Query** for data fetching
- **Zustand** for state management
- **Axios** for API calls
- **Lucide React** for icons

## Running Standalone

### Prerequisites
- Node.js 20+

### Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend will be available at http://localhost:3000

## Pages

| Route | Description |
|---|---|
| `/dashboard` | Overview stats and activity charts |
| `/datasets` | Dataset management and upload |
| `/jobs` | Job monitoring and status |
| `/agent` | AI chat interface |
| `/workflows` | Pipeline builder |
| `/settings` | LLM provider configuration |
