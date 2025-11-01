# SBA Certifications Search Actor

This actor calls the official SBA Small Business Search API (`https://search.certifications.sba.gov/_api/v2/search`) and saves the returned businesses to the default dataset. You can reproduce the filters available on the public website and optionally enrich each result with the detailed profile endpoint.

## Input

Key fields (see `actor/input_schema.json` for the full list):

- `searchTerm` – full text search across name, UEI, CAGE code, keywords, etc.
- `states` – array of two-letter state / territory codes. At least one filter (state, certification, NAICS, keyword, etc.) must be provided; the server rejects completely empty searches.
- `sbaCertifications`, `selfCertifications`, `qualityStandards` – use the same values shown on the website filter controls.
- `naicsCodes` – array of NAICS code strings or objects `{ "code": "...", "label": "..." }`.
- `keywords` – keyword list with optional `keywordOperator` (`Or`/`And`).
- `lastUpdated` – `"anytime"`, `"past-3-months"`, `"past-6-months"`, `"past-year"`, or `"custom"` with `customDateRange`.
- `samActive`, bonding level limits, employee count, revenue, and `entityDetailId` are supported just like on the site.
- `includeProfiles` – fetch `/_api/v2/profile/{uei}/{cage}` for each result (default `false`). Use `profileConcurrency` to limit concurrent detail requests.
- `maxItems` – trim the returned array (0 keeps everything the API sends back).
- `requestTimeoutSecs` – HTTP timeout per call (default 60 seconds).
- `proxyConfiguration` – optional Apify proxy settings.

Example input:

```json
{
  "searchTerm": "construction",
  "states": ["ND"],
  "sbaCertifications": ["3"],
  "naicsCodes": ["237130", "237990"],
  "includeProfiles": true,
  "maxItems": 100
}
```

## Output

- Default dataset contains one item per returned business, including:
  - Normalised fields (business name, UEI, CAGE, contact, location, NAICS codes, certifications, bonding levels, revenue, etc.).
  - `lastUpdateDateIso` – ISO timestamp converted from the epoch value.
  - `profile` + `profileError` if `includeProfiles` was enabled.
  - `raw` – the untouched record from the `/_api/v2/search` response.
- Key-value store record `OUTPUT` summarises total results, number stored, the exact filters sent to the API, and the `meili_filter` blob returned by the service.

## Notes

- The SBA API returns HTTP 500 when no filters are applied; the actor checks this upfront and fails fast with a descriptive message.
- When using `includeProfiles`, keep `profileConcurrency` reasonable to avoid overloading the SBA service.
