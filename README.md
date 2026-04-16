# Raleigh Builder Intelligence

A public dashboard for researching residential builders in Raleigh, NC.
Built on the City of Raleigh's open building permit data.

## Running locally

```bash
cd raleigh-dashboard
python3 -m http.server 8000
# visit http://localhost:8000
```

Requires Python 3. No other dependencies for the frontend.

## Project structure

```
raleigh-dashboard/
  index.html                        # Main app
  raleigh-permits-api.js            # Data pipeline & aggregation
  favicon.png                       # App icon
  package.json                      # Marks project as ES module (needed for fetch script)
  data/
    permits.json                    # Cached permit data — updated nightly by GitHub Action
  scripts/
    fetch-permits.js                # Node.js script that fetches from ArcGIS and writes permits.json
  .github/
    workflows/
      update-data.yml               # GitHub Action: runs fetch-permits.js every night at midnight UTC
```

## How data caching works

The app tries to load `data/permits.json` first. If the file exists and is
less than 48 hours old, it uses that — load time drops from ~20s to under 1s.
If the cache is missing or stale, it falls back to fetching live from ArcGIS.

The GitHub Action runs nightly at midnight UTC, fetches all permits, and
commits the updated `data/permits.json` back to the repository. Netlify then
automatically redeploys the site with the fresh data.

## Deploying to Netlify

1. Push this folder to a GitHub repository
2. Go to [netlify.com](https://netlify.com) → New site → Import from GitHub
3. Select your repository
4. Build command: *(leave blank)*
5. Publish directory: `.` (the root)
6. Click Deploy

Netlify will serve the static files. No build step required.

## Running the fetch script manually

```bash
node scripts/fetch-permits.js
```

This writes `data/permits.json`. Useful for testing or forcing a refresh
before the nightly Action runs.

## Custom domain

In Netlify: Site settings → Domain management → Add custom domain.
Buy a domain from Namecheap or Google Domains (~$12/year), then follow
Netlify's DNS instructions.

## Data source

City of Raleigh Building Permits — ArcGIS Feature Service  
`https://data-ral.opendata.arcgis.com/datasets/building-permits`  
Updated daily by the City of Raleigh. Public domain.
