# SBA Certifications Search Actor

This actor calls the official SBA Small Business Search API (`https://search.certifications.sba.gov/_api/v2/search`) and saves the returned businesses to the default dataset. You can reproduce the filters available on the public website and optionally enrich each result with the detailed profile endpoint.

## Input

Key fields (see `.actor/input_schema.json` for the full list and defaults):

- `searchTerm` – optional full text search across name, UEI, CAGE code, keywords, etc.
- `states` – array of two-letter state / territory codes. At least one filter (search term or state) must be provided; the SBA API rejects completely empty searches.
- `lastUpdated` – `"anytime"`, `"past-3-months"`, `"past-6-months"`, `"past-year"`, or `"custom"` with `customDateRange`.
- `maxItems` – trim the returned array (0 keeps everything the API sends back).

Example input:

```json
{
  "states": ["ND"],
  "lastUpdated": "past-year",
  "maxItems": 25
}
```

## Docker Build

This project follows the standard Apify Docker flow (`Dockerfile` in the repository root). Build and test the image locally with:

```bash
docker build -t sba-certifications-search-actor .
```

Run the actor inside the container by providing the usual Apify input JSON:

```bash
docker run --rm -e APIFY_INPUT='{"searchTerm":"construction","states":["ND"]}' sba-certifications-search-actor
```

## Output

- Default dataset contains one item per returned business, including:
  - Normalised fields (business name, UEI, CAGE, contact, location, NAICS codes, certifications, bonding levels, revenue, etc.).
  - `lastUpdateDateIso` – ISO timestamp converted from the epoch value.
  - `profile` + `profileError` if `includeProfiles` was enabled.
- `raw` – the untouched record from the `/_api/v2/search` response.
- The complete dataset contract is defined in `.actor/dataset_schema.json`.
- Key-value store record `OUTPUT` summarises total results, number stored, the exact filters sent to the API, and the `meili_filter` blob returned by the service.

## Notes

- The SBA API returns HTTP 500 when no filters are applied; the actor checks this upfront and fails fast with a descriptive message.
- When using `includeProfiles`, keep `profileConcurrency` reasonable to avoid overloading the SBA service.
