from tara_migrate.core.config import (  # noqa: F401
    AR_DIR,
    EN_DIR,
    FILE_MAP_FILE,
    ID_MAP_FILE,
    SOURCE_DIR,
    SPAIN_DIR,
    get_dest_access_token,
    get_dest_shop_url,
    get_magento_site_url,
    get_magento_store_code,
    get_source_access_token,
    get_source_shop_url,
)
from tara_migrate.core.utils import (  # noqa: F401
    ARTICLE_FILE_METAFIELDS,
    DEFINITION_ORDER,
    FILE_FIELD_PRESETS,
    IMAGE_KEYWORDS,
    MAGENTO_HEADERS,
    METAOBJECT_FILE_FIELDS,
    REQUEST_DELAY,
    SECTION_PRESETS,
    ascii_slugify,
    load_json,
    sanitize_rich_text_json,
    save_json,
    sort_by_dependency,
    unicode_slugify,
)
# New shared modules
from tara_migrate.core.language import (  # noqa: F401
    count_chars,
    detect_mixed_language,
    has_arabic,
    has_significant_english,
    is_arabic_visible_text,
)
from tara_migrate.core.rich_text import (  # noqa: F401
    extract_text as extract_rich_text,
    extract_text_nodes as extract_rich_text_nodes,
    is_rich_text_json,
    rebuild as rebuild_rich_text,
    sanitize as sanitize_rich_text,
    validate_json,
)
from tara_migrate.core.shopify_fields import (  # noqa: F401
    SKIP_FIELD_PATTERNS,
    TEXT_METAFIELD_TYPES,
    TRANSLATABLE_RESOURCE_TYPES,
    is_skippable_field,
    is_skippable_value,
)
from tara_migrate.core.csv_utils import (  # noqa: F401
    ARABIC_LOCALE,
    CSV_TYPE_TO_GID,
    NEEDS_PARENT_RESOLUTION,
    SKIP_TYPES,
    classify_row,
    is_keep_as_is,
    is_non_translatable,
)
from tara_migrate.core.graphql_queries import (  # noqa: F401
    FETCH_DIGESTS_QUERY,
    FETCH_METAOBJECTS_QUERY,
    FETCH_PRODUCTS_QUERY,
    FETCH_THEME_DIGESTS_QUERY,
    REGISTER_TRANSLATIONS_MUTATION,
    TRANSLATABLE_RESOURCES_QUERY,
    fetch_translatable_resources,
    paginate_query,
    upload_translations,
)
