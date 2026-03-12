/**
 * Feature flags for UI features not yet supported by the API.
 * Flip to `true` when the corresponding API endpoint is implemented.
 */

/** Cluster Status page — GET /nodes endpoint now available */
export const FEATURE_CLUSTER_STATUS = true;

/** Dashboard: active nodes metric card */
export const FEATURE_DASHBOARD_ACTIVE_NODES = true;

/** Dashboard: power draw metric card */
export const FEATURE_DASHBOARD_POWER_DRAW = false;

/** Dashboard: session cost metric card */
export const FEATURE_DASHBOARD_SESSION_COST = false;

/** Dashboard: activity log */
export const FEATURE_DASHBOARD_ACTIVITY_LOG = false;

/** Job Queue: priority, deadline, epochs, assigned node columns */
export const FEATURE_JOB_EXTENDED_FIELDS = true;

/** Submit Job: priority slider, deadline picker, batch size, epochs, script path */
export const FEATURE_SUBMIT_EXTENDED_FIELDS = true;

/** Docker image upload (exists in API) */
export const FEATURE_IMAGE_UPLOAD = true;

/** Profiling scheduler: show GPU config, ETA, and profiling badge */
export const FEATURE_PROFILING_SCHEDULER = true;

/** Profiling page: dedicated page showing profiling results per job */
export const FEATURE_PROFILING_PAGE = true;
