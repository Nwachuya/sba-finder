import { Actor, log } from 'apify';
import { gotScraping } from 'got-scraping';
import pLimit from 'p-limit';
import { z } from 'zod';

const API_BASE_URL = 'https://search.certifications.sba.gov/_api/v2';

const STATE_DATA = [
    ['AL', 'Alabama'],
    ['AK', 'Alaska'],
    ['AS', 'American Samoa'],
    ['AZ', 'Arizona'],
    ['AR', 'Arkansas'],
    ['CA', 'California'],
    ['CO', 'Colorado'],
    ['CT', 'Connecticut'],
    ['DE', 'Delaware'],
    ['DC', 'District of Columbia'],
    ['FM', 'Federated States of Micronesia'],
    ['FL', 'Florida'],
    ['GA', 'Georgia'],
    ['GU', 'Guam'],
    ['HI', 'Hawaii'],
    ['ID', 'Idaho'],
    ['IL', 'Illinois'],
    ['IN', 'Indiana'],
    ['IA', 'Iowa'],
    ['KS', 'Kansas'],
    ['KY', 'Kentucky'],
    ['LA', 'Louisiana'],
    ['ME', 'Maine'],
    ['MH', 'Marshall Islands'],
    ['MD', 'Maryland'],
    ['MA', 'Massachusetts'],
    ['MI', 'Michigan'],
    ['MN', 'Minnesota'],
    ['MS', 'Mississippi'],
    ['MO', 'Missouri'],
    ['MT', 'Montana'],
    ['NE', 'Nebraska'],
    ['NV', 'Nevada'],
    ['NH', 'New Hampshire'],
    ['NJ', 'New Jersey'],
    ['NM', 'New Mexico'],
    ['NY', 'New York'],
    ['NC', 'North Carolina'],
    ['ND', 'North Dakota'],
    ['MP', 'Northern Mariana Islands'],
    ['OH', 'Ohio'],
    ['OK', 'Oklahoma'],
    ['OR', 'Oregon'],
    ['PW', 'Palau'],
    ['PA', 'Pennsylvania'],
    ['PR', 'Puerto Rico'],
    ['RI', 'Rhode Island'],
    ['SC', 'South Carolina'],
    ['SD', 'South Dakota'],
    ['TN', 'Tennessee'],
    ['TX', 'Texas'],
    ['UT', 'Utah'],
    ['VT', 'Vermont'],
    ['VA', 'Virginia'],
    ['VI', 'U.S. Virgin Islands'],
    ['WA', 'Washington'],
    ['WV', 'West Virginia'],
    ['WI', 'Wisconsin'],
    ['WY', 'Wyoming'],
];

const SBA_CERTIFICATIONS = [
    { value: '1,4', label: '8(a) or 8(a) Joint Venture' },
    { value: '3', label: 'HUBZone' },
    { value: '5', label: 'Women-Owned Small Business (WOSB)' },
    { value: '6', label: 'Economically-Disadvantaged Women-Owned Small Business (EDWOSB)' },
    { value: '7,8', label: 'Veteran-Owned Small Business (VOSB)' },
    { value: '9,10', label: 'Service-Disabled Veteran-Owned Small Business (SDVOSB)' },
];

const SELF_CERTIFICATIONS = [
    { value: 'Self-Certified Small Disadvantaged Business', label: 'Self-Certified Small Disadvantaged Business' },
    { value: 'HUBZone Joint Venture', label: 'Self-Certified HUBZone Joint Venture' },
    { value: 'Veteran Owned Business', label: 'Self-Certified Veteran-Owned Small Business' },
    { value: 'Women-Owned Small Business', label: 'Self-Certified Woman-Owned Small Business' },
    { value: 'Women-Owned Small Business Joint Venture', label: 'Self-Certified Woman-Owned Small Business Joint Venture' },
    { value: 'Community Development Corporation Owned Firm', label: 'Community Development Corporation (CDC) Owned Small Business' },
    { value: 'Native American Owned', label: 'Native American Owned' },
    { value: 'Tribally Owned Firm', label: 'Tribally Owned Small Business' },
    { value: 'American Indian Owned', label: 'American Indian Owned Small Business' },
    { value: 'Alaskan Native Corporation Owned Firm', label: 'Alaskan Native Corp (ANC) Owned Small Business' },
    { value: 'Native Hawaiian Organization Owned Firm', label: 'Native Hawaiian Org (NHO) Owned Small Business' },
];

const QUALITY_ASSURANCE_STANDARDS = [
    { value: 'ANSI/ASQC Z1.4', label: 'ANSI/ASQC Z1.4' },
    { value: 'ISO-9000 Series', label: 'ISO 9000 Series' },
    { value: 'ISO 10012-1', label: 'ISO 10012-1' },
    { value: 'MIL-Q-9858', label: 'MIL-Q-9858' },
    { value: 'MIL-STD-45662A', label: 'MIL-STD-45662A' },
];

const LAST_UPDATED_OPTIONS = {
    anytime: { label: 'Anytime', value: 'anytime' },
    'past-3-months': { label: 'Within the past 3 months', value: 'past-3-months' },
    'past-6-months': { label: 'Within the past 6 months', value: 'past-6-months' },
    'past-year': { label: 'Within the past year', value: 'past-year' },
    custom: { label: 'Custom', value: 'custom' },
};

const BASE_FILTERS = Object.freeze({
    searchProfiles: { searchTerm: '' },
    location: { states: [], zipCodes: [], counties: [], districts: [], msas: [] },
    sbaCertifications: { activeCerts: [], isPreviousCert: false, operatorType: 'Or' },
    naics: { codes: [], isPrimary: false, operatorType: 'Or' },
    selfCertifications: { certifications: [], operatorType: 'Or' },
    keywords: { list: [], operatorType: 'Or' },
    lastUpdated: { date: LAST_UPDATED_OPTIONS.anytime },
    samStatus: { isActiveSAM: false },
    qualityAssuranceStandards: { qas: [] },
    bondingLevels: { constructionIndividual: '', constructionAggregate: '', serviceIndividual: '', serviceAggregate: '' },
    businessSize: { relationOperator: 'at-least', numberOfEmployees: '' },
    annualRevenue: { relationOperator: 'at-least', annualGrossRevenue: '' },
    entityDetailId: '',
});

const InputSchema = z
    .object({
        searchTerm: z.string().trim().min(1).optional(),
        states: z.array(z.string().trim().min(2)).optional(),
        zipCodes: z.array(z.string().trim().min(1)).optional(),
        sbaCertifications: z.array(z.string().trim().min(1)).optional(),
        sbaCertificationsOperator: z.enum(['Or', 'And']).optional(),
        sbaCertificationsIncludePrevious: z.boolean().optional(),
        naicsCodes: z
            .array(
                z.union([
                    z.string().trim().min(1),
                    z
                        .object({
                            code: z.string().trim().min(1),
                            label: z.string().trim().min(1).optional(),
                        })
                        .strict(),
                ]),
            )
            .optional(),
        naicsIsPrimary: z.boolean().optional(),
        naicsOperator: z.enum(['Or', 'And']).optional(),
        selfCertifications: z.array(z.string().trim().min(1)).optional(),
        keywords: z.array(z.string().trim().min(1)).optional(),
        keywordOperator: z.enum(['Or', 'And']).optional(),
        lastUpdated: z.enum(['anytime', 'past-3-months', 'past-6-months', 'past-year', 'custom']).optional(),
        customDateRange: z
            .object({
                from: z.string().trim().min(1),
                to: z.string().trim().min(1),
            })
            .optional(),
        samActive: z.boolean().optional(),
        qualityStandards: z.array(z.string().trim().min(1)).optional(),
        bondingLevels: z
            .object({
                constructionIndividual: z.number().nonnegative().optional(),
                constructionAggregate: z.number().nonnegative().optional(),
                serviceIndividual: z.number().nonnegative().optional(),
                serviceAggregate: z.number().nonnegative().optional(),
            })
            .optional(),
        businessSize: z
            .object({
                relation: z.enum(['at-least', 'no-more']).optional(),
                numberOfEmployees: z.number().nonnegative().optional(),
            })
            .optional(),
        annualRevenue: z
            .object({
                relation: z.enum(['at-least', 'no-more']).optional(),
                annualGrossRevenue: z.number().nonnegative().optional(),
            })
            .optional(),
        entityDetailId: z.union([z.string().trim(), z.number()]).optional(),
        includeProfiles: z.boolean().optional(),
        profileConcurrency: z.number().int().positive().max(10).optional(),
        maxItems: z.number().int().nonnegative().optional(),
        requestTimeoutSecs: z.number().positive().optional(),
        proxyConfiguration: z.any().optional(),
    })
    .strict();

const stateOptionByCode = new Map();
const stateOptionByValue = new Map();
const stateOptionByName = new Map();

for (const [code, name] of STATE_DATA) {
    const option = { value: `${code} - ${name}`, label: `${name} (${code})` };
    stateOptionByCode.set(code.toUpperCase(), option);
    stateOptionByValue.set(option.value, option);
    stateOptionByName.set(name.toLowerCase(), option);
}

const sbaCertByValue = new Map(SBA_CERTIFICATIONS.map((opt) => [opt.value, opt]));
const sbaCertByLabel = new Map(SBA_CERTIFICATIONS.map((opt) => [opt.label.toLowerCase(), opt]));

const selfCertByValue = new Map(SELF_CERTIFICATIONS.map((opt) => [opt.value, opt]));
const selfCertByLabel = new Map(SELF_CERTIFICATIONS.map((opt) => [opt.label.toLowerCase(), opt]));

const qualityByValue = new Map(QUALITY_ASSURANCE_STANDARDS.map((opt) => [opt.value, opt]));
const qualityByLabel = new Map(QUALITY_ASSURANCE_STANDARDS.map((opt) => [opt.label.toLowerCase(), opt]));

const deepClone = (obj) => JSON.parse(JSON.stringify(obj));

const toSimpleOption = (value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
        const val = typeof value.value === 'string' ? value.value.trim() : '';
        if (!val) throw new Error('Invalid item: missing value');
        return { value: val, label: typeof value.label === 'string' && value.label.trim() ? value.label.trim() : val };
    }
    const stringValue = String(value ?? '').trim();
    if (!stringValue) throw new Error('Invalid item: empty value');
    return { value: stringValue, label: stringValue };
};

const normalizeState = (input) => {
    if (!input) throw new Error('Empty state value');
    if (typeof input === 'object' && !Array.isArray(input)) {
        if (typeof input.value === 'string' && typeof input.label === 'string') {
            const val = input.value.trim();
            if (stateOptionByValue.has(val)) return { ...stateOptionByValue.get(val) };
            throw new Error(`Unknown state value: ${val}`);
        }
        if (typeof input.code === 'string') return { ...resolveStateByCode(input.code) };
    }
    const trimmed = String(input).trim();
    if (!trimmed) throw new Error('Empty state value');
    if (stateOptionByValue.has(trimmed)) return { ...stateOptionByValue.get(trimmed) };
    if (stateOptionByCode.has(trimmed.toUpperCase())) return { ...stateOptionByCode.get(trimmed.toUpperCase()) };
    if (stateOptionByName.has(trimmed.toLowerCase())) return { ...stateOptionByName.get(trimmed.toLowerCase()) };
    throw new Error(`Unknown state value: ${input}`);
};

const resolveStateByCode = (code) => {
    const normalized = code.trim().toUpperCase();
    if (!stateOptionByCode.has(normalized)) throw new Error(`Unknown state code: ${code}`);
    return stateOptionByCode.get(normalized);
};

const dedupeByValue = (items) => {
    const seen = new Set();
    const output = [];
    for (const item of items) {
        if (!seen.has(item.value)) {
            seen.add(item.value);
            output.push(item);
        }
    }
    return output;
};

const mapCertification = (item) => {
    if (typeof item === 'object' && !Array.isArray(item) && typeof item.value === 'string') {
        const value = item.value.trim();
        const base = sbaCertByValue.get(value);
        return base ? { ...base } : { value, label: item.label?.trim() || value };
    }
    const text = String(item ?? '').trim();
    if (sbaCertByValue.has(text)) return { ...sbaCertByValue.get(text) };
    if (sbaCertByLabel.has(text.toLowerCase())) return { ...sbaCertByLabel.get(text.toLowerCase()) };
    throw new Error(`Unsupported SBA certification: ${item}`);
};

const mapSelfCertification = (item) => {
    if (typeof item === 'object' && !Array.isArray(item) && typeof item.value === 'string') {
        const { value } = item;
        if (selfCertByValue.has(value)) return { ...selfCertByValue.get(value) };
        return { value: value.trim(), label: item.label?.trim() || value.trim() };
    }
    const text = String(item ?? '').trim();
    if (selfCertByValue.has(text)) return { ...selfCertByValue.get(text) };
    if (selfCertByLabel.has(text.toLowerCase())) return { ...selfCertByLabel.get(text.toLowerCase()) };
    throw new Error(`Unsupported self certification: ${item}`);
};

const mapQualityStandard = (item) => {
    if (typeof item === 'object' && !Array.isArray(item) && typeof item.value === 'string') {
        const val = item.value.trim();
        if (qualityByValue.has(val)) return { ...qualityByValue.get(val) };
        return { value: val, label: item.label?.trim() || val };
    }
    const text = String(item ?? '').trim();
    if (qualityByValue.has(text)) return { ...qualityByValue.get(text) };
    if (qualityByLabel.has(text.toLowerCase())) return { ...qualityByLabel.get(text.toLowerCase()) };
    throw new Error(`Unsupported quality assurance standard: ${item}`);
};

const mapNaicsCode = (item) => {
    if (typeof item === 'object' && !Array.isArray(item)) {
        const code = typeof item.code === 'string' ? item.code.trim() : typeof item.value === 'string' ? item.value.trim() : '';
        if (!code) throw new Error('NAICS code missing value');
        return { value: code, label: typeof item.label === 'string' && item.label.trim() ? item.label.trim() : code };
    }
    const code = String(item ?? '').trim();
    if (!code) throw new Error('NAICS code missing value');
    return { value: code, label: code };
};

const mapKeyword = (keyword) => {
    const value = String(keyword ?? '').trim();
    if (!value) throw new Error('Keyword cannot be empty');
    return { value, label: value };
};

const resolveLastUpdated = (option, customRange) => {
    if (!option) return null;
    if (option === 'custom') {
        if (!customRange) throw new Error('customDateRange is required when lastUpdated is set to custom');
        const fromDate = parseDate(customRange.from);
        const toDate = parseDate(customRange.to);
        if (!fromDate || !toDate) throw new Error('customDateRange must contain valid dates');
        if (fromDate > toDate) throw new Error('customDateRange.from must be before customDateRange.to');
        return { label: 'Custom', value: `${fromDate.toISOString()}-${toDate.toISOString()}` };
    }
    const preset = LAST_UPDATED_OPTIONS[option];
    if (!preset) throw new Error(`Unknown lastUpdated option: ${option}`);
    return preset;
};

const parseDate = (input) => {
    const date = new Date(input);
    return Number.isNaN(date.getTime()) ? null : date;
};

const isCustomRangeValid = (value) => {
    if (typeof value !== 'string' || !value.includes('-')) return false;
    const [from, to] = value.split('-');
    if (!from || !to) return false;
    const fromDate = parseDate(from);
    const toDate = parseDate(to);
    return Boolean(fromDate && toDate);
};

const countActiveFilters = (filters) => {
    let total = 0;
    const loc = filters.location;
    const locationCount =
        loc.states.length + loc.zipCodes.length + loc.counties.length + loc.districts.length + loc.msas.length;
    total += locationCount;
    if (filters.searchProfiles.searchTerm.trim()) total += 1;
    total += filters.sbaCertifications.activeCerts.length;
    total += filters.naics.codes.length;
    total += filters.selfCertifications.certifications.length;
    total += filters.keywords.list.length;
    const lastUpdated = filters.lastUpdated.date;
    if (lastUpdated.value !== LAST_UPDATED_OPTIONS.anytime.value) {
        if (lastUpdated.label === 'Custom') total += isCustomRangeValid(lastUpdated.value) ? 1 : 0;
        else total += 1;
    }
    if (filters.samStatus.isActiveSAM) total += 1;
    total += filters.qualityAssuranceStandards.qas.length;
    total += Object.values(filters.bondingLevels).filter((val) => val !== '' && val !== null && val !== undefined).length;
    if (filters.businessSize.numberOfEmployees) total += 1;
    if (filters.annualRevenue.annualGrossRevenue) total += 1;
    if (filters.entityDetailId) total += 1;
    return total;
};

const buildFilters = (input) => {
    const filters = deepClone(BASE_FILTERS);
    if (input.searchTerm) filters.searchProfiles.searchTerm = input.searchTerm.trim();

    if (input.states?.length) {
        const states = input.states.map(normalizeState);
        filters.location.states = dedupeByValue(states);
    }

    if (input.zipCodes?.length) {
        const zipCodes = input.zipCodes.map((zip) => toSimpleOption(zip));
        filters.location.zipCodes = dedupeByValue(zipCodes);
    }

    if (input.sbaCertifications?.length) {
        const certs = input.sbaCertifications.map(mapCertification);
        filters.sbaCertifications.activeCerts = dedupeByValue(certs);
    }
    if (typeof input.sbaCertificationsIncludePrevious === 'boolean') {
        filters.sbaCertifications.isPreviousCert = input.sbaCertificationsIncludePrevious;
    }
    if (input.sbaCertificationsOperator) filters.sbaCertifications.operatorType = input.sbaCertificationsOperator;

    if (input.naicsCodes?.length) {
        const naics = input.naicsCodes.map(mapNaicsCode);
        filters.naics.codes = dedupeByValue(naics);
    }
    if (typeof input.naicsIsPrimary === 'boolean') filters.naics.isPrimary = input.naicsIsPrimary;
    if (input.naicsOperator) filters.naics.operatorType = input.naicsOperator;

    if (input.selfCertifications?.length) {
        const selfCerts = input.selfCertifications.map(mapSelfCertification);
        filters.selfCertifications.certifications = dedupeByValue(selfCerts);
    }

    if (input.keywords?.length) {
        const keywords = input.keywords.map(mapKeyword);
        filters.keywords.list = dedupeByValue(keywords);
    }
    if (input.keywordOperator) filters.keywords.operatorType = input.keywordOperator;

    const lastUpdated = resolveLastUpdated(input.lastUpdated ?? 'anytime', input.customDateRange);
    if (lastUpdated) filters.lastUpdated.date = lastUpdated;

    if (typeof input.samActive === 'boolean') filters.samStatus.isActiveSAM = input.samActive;

    if (input.qualityStandards?.length) {
        const qas = input.qualityStandards.map(mapQualityStandard);
        filters.qualityAssuranceStandards.qas = dedupeByValue(qas);
    }

    if (input.bondingLevels) {
        for (const key of Object.keys(filters.bondingLevels)) {
            if (key in input.bondingLevels && input.bondingLevels[key] !== undefined) {
                filters.bondingLevels[key] = String(input.bondingLevels[key]);
            }
        }
    }

    if (input.businessSize) {
        if (typeof input.businessSize.relation === 'string') {
            filters.businessSize.relationOperator = input.businessSize.relation;
        }
        if (input.businessSize.numberOfEmployees !== undefined) {
            filters.businessSize.numberOfEmployees = String(input.businessSize.numberOfEmployees);
        }
    }

    if (input.annualRevenue) {
        if (typeof input.annualRevenue.relation === 'string') {
            filters.annualRevenue.relationOperator = input.annualRevenue.relation;
        }
        if (input.annualRevenue.annualGrossRevenue !== undefined) {
            filters.annualRevenue.annualGrossRevenue = String(input.annualRevenue.annualGrossRevenue);
        }
    }

    if (input.entityDetailId !== undefined) filters.entityDetailId = String(input.entityDetailId).trim();

    return filters;
};

const toIsoFromEpoch = (value) => {
    if (value === null || value === undefined) return null;
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) return null;
    return new Date(num * 1000).toISOString();
};

const fetchSearchResults = async (filters, options) => {
    const timeoutMs = Math.round((options.requestTimeoutSecs ?? 60) * 1000);
    const requestOptions = {
        url: `${API_BASE_URL}/search`,
        method: 'POST',
        responseType: 'json',
        headers: {
            'content-type': 'application/json',
            accept: 'application/json',
        },
        json: filters,
        timeout: {
            request: timeoutMs,
        },
        retry: {
            limit: 2,
        },
    };
    if (options.proxyConfiguration) {
        requestOptions.proxyUrl = await options.proxyConfiguration.newUrl();
    }
    const response = await gotScraping(requestOptions);
    return response.body;
};

const fetchProfile = async (result, options) => {
    const { uei, cage_code: cageCode } = result;
    if (!uei || !cageCode) return { profile: null, error: 'Missing UEI or CAGE code' };
    const timeoutMs = Math.round((options.requestTimeoutSecs ?? 60) * 1000);
    const requestOptions = {
        url: `${API_BASE_URL}/profile/${uei}/${cageCode}`,
        responseType: 'json',
        headers: {
            accept: 'application/json',
        },
        timeout: {
            request: timeoutMs,
        },
        retry: {
            limit: 1,
        },
    };
    if (options.proxyConfiguration) {
        requestOptions.proxyUrl = await options.proxyConfiguration.newUrl();
    }
    try {
        const response = await gotScraping(requestOptions);
        return { profile: response.body ?? null, error: null };
    } catch (error) {
        log.warning(`Profile request failed for UEI ${uei}, CAGE ${cageCode}: ${error.message}`);
        return { profile: null, error: error.message };
    }
};

const transformResult = (result, profileInfo) => {
    const lastUpdateIso = toIsoFromEpoch(result.last_update_date);
    const bonding = {
        constructionPerContract: result.construction_bonding_per_contract ?? null,
        constructionAggregate: result.construction_bonding_aggregate ?? null,
        servicePerContract: result.service_bonding_contract ?? null,
        serviceAggregate: result.service_bonding_aggregate ?? null,
    };
    const contact = {
        name: result.contact_person ?? null,
        phone: result.display_phone ?? result.phone ?? null,
        email: result.display_email ?? result.email ?? null,
        fax: result.display_fax ?? result.fax ?? null,
    };
    const location = {
        address1: result.address_1 ?? null,
        address2: result.address_2 ?? null,
        city: result.city ?? null,
        state: result.state ?? null,
        zipcode: result.zipcode ?? null,
        county: result.county ?? null,
        countyCode: result.county_code ?? null,
        fipsCode: result.fips_code ?? null,
        congressionalDistrict: result.congressional_district ?? null,
        msa: result.msa ?? null,
    };
    const exporter = {
        status: result.exporter_status ?? null,
        activities: result.export_business_activities ?? null,
        exportTo: result.export_to ?? null,
        desiredRelationships: result.desired_export_relationships ?? null,
        objective: result.export_objective ?? null,
    };
    return {
        entityDetailId: result.entity_detail_id ?? null,
        uei: result.uei ?? null,
        cageCode: result.cage_code ?? null,
        businessName: result.legal_business_name ?? null,
        dbaName: result.dba_name ?? null,
        samExtractCode: result.sam_extract_code ?? null,
        contact,
        location,
        website: result.website ?? null,
        additionalWebsite: result.additional_website ?? null,
        capabilitiesNarrative: result.capabilities_narrative ?? null,
        capabilitiesLink: result.capabilities_link ?? null,
        naicsPrimary: result.naics_primary ?? null,
        naicsAllCodes: result.naics_all_codes ?? [],
        naicsSmallCodes: result.naics_small_codes ?? [],
        naicsExceptionCodes: result.naics_exception_codes ?? [],
        sbaCertifications: result.certs ?? [],
        selfCertifications: result.meili_self_certifications ?? [],
        qualityAssuranceStandards: result.qas_standards ?? [],
        bondingLevels: bonding,
        exporter,
        businessSize: result.business_size ?? null,
        annualRevenue: result.annual_revenue ?? null,
        yearEstablished: result.year_established ?? null,
        lastUpdateDateUnix: result.last_update_date ?? null,
        lastUpdateDateIso: lastUpdateIso,
        rankingScore: result._rankingScore ?? null,
        profile: profileInfo?.profile ?? null,
        profileError: profileInfo?.error ?? null,
        raw: result,
    };
};

await Actor.init();

try {
    let envInput = null;
    if (process.env.APIFY_INPUT) {
        try {
            envInput = JSON.parse(process.env.APIFY_INPUT);
        } catch (error) {
            log.warning(`Failed to parse APIFY_INPUT environment variable: ${error.message}`);
        }
    }
    const rawInput = (await Actor.getInput()) ?? envInput ?? {};
    const input = InputSchema.parse(rawInput);

    const proxyConfiguration = input.proxyConfiguration
        ? await Actor.createProxyConfiguration(input.proxyConfiguration)
        : null;

    const filters = buildFilters(input);
    const activeFilterCount = countActiveFilters(filters);
    if (activeFilterCount === 0) {
        throw new Error('At least one filter must be provided (search term, state, certification, NAICS code, etc.).');
    }

    log.info(`Fetching SBA businesses with ${activeFilterCount} active filter(s).`);
    const searchResponse = await fetchSearchResults(filters, {
        proxyConfiguration,
        requestTimeoutSecs: input.requestTimeoutSecs ?? 60,
    });

    if (searchResponse?.error) throw new Error(`API error: ${searchResponse.error}`);
    const results = Array.isArray(searchResponse?.results) ? searchResponse.results : [];
    log.info(`API returned ${results.length} businesses.`);

    const maxItems = input.maxItems ?? 0;
    const slicedResults = maxItems > 0 ? results.slice(0, maxItems) : results;

    let profileDetails = [];
    if (input.includeProfiles && slicedResults.length > 0) {
        const concurrency = input.profileConcurrency ?? 3;
        log.info(`Fetching detailed profiles for ${slicedResults.length} businesses (concurrency ${concurrency}).`);
        const limit = pLimit(concurrency);
        profileDetails = await Promise.all(
            slicedResults.map((result) =>
                limit(() =>
                    fetchProfile(result, {
                        proxyConfiguration,
                        requestTimeoutSecs: input.requestTimeoutSecs ?? 60,
                    }),
                ),
            ),
        );
    }

    const datasetItems = slicedResults.map((result, index) =>
        transformResult(result, profileDetails[index]),
    );

    if (datasetItems.length) {
        await Actor.pushData(datasetItems);
        log.info(`Stored ${datasetItems.length} businesses to the default dataset.`);
    } else {
        log.info('No items matched the provided filters.');
    }

    await Actor.setValue('OUTPUT', {
        totalResults: results.length,
        exported: datasetItems.length,
        filtersApplied: filters,
        meiliFilter: searchResponse?.meili_filter ?? null,
    });
} catch (error) {
    log.exception(error, 'Actor failed.');
    throw error;
} finally {
    await Actor.exit();
}
