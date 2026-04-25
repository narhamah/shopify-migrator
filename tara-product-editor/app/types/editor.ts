export type LocaleCode = "en" | "ar";

export type MetafieldValueType =
  | "single_line_text_field"
  | "multi_line_text_field"
  | "rich_text_field"
  | "number_integer"
  | "number_decimal"
  | "boolean"
  | "json"
  | "file_reference"
  | "list.file_reference"
  | "url"
  | "product_reference"
  | "collection_reference"
  | "metaobject_reference"
  | "list.product_reference"
  | "list.collection_reference"
  | "list.metaobject_reference"
  | "string";

export interface MetafieldDefinitionSummary {
  id: string;
  namespace: string;
  key: string;
  name: string;
  description?: string | null;
  type: MetafieldValueType | string;
}

export interface TranslationMapping {
  id: string;
  label: string;
  namespace?: string;
  key: string;
  type: "product" | "metafield";
  sourceResourceType: "PRODUCT" | "METAFIELD";
  resourceKey: string;
  translationKey: string;
  fieldType: MetafieldValueType | "product_field";
  group: string;
}

export interface ImageStorageTarget {
  id: string;
  label: string;
  locale: LocaleCode;
  storage: "metafield" | "translation";
  namespace: string;
  key: string;
  type: "file_reference" | "list.file_reference";
  translationKey?: string;
  sourceMetafield?: string;
}

export interface ImageGroupMapping {
  id: string;
  label: string;
  notes: string[];
  english: ImageStorageTarget[];
  arabic: ImageStorageTarget[];
  legacy: ImageStorageTarget[];
}

export interface ProductContentMapping {
  namespace: string;
  key: string;
  label: string;
  type: MetafieldValueType;
  group: string;
}

export interface DiscoveryConfig {
  schemaVersion: number;
  shop: string;
  generatedAt: string;
  sampledProducts: number;
  definitionCount: number;
  notes: string[];
  rawDefinitions: MetafieldDefinitionSummary[];
  productContentMappings: {
    coreFields: Array<{
      key:
        | "title"
        | "handle"
        | "descriptionHtml"
        | "vendor"
        | "productType"
        | "tags"
        | "status"
        | "seo.title"
        | "seo.description";
      label: string;
    }>;
    metafields: ProductContentMapping[];
  };
  translationMappings: {
    product: TranslationMapping[];
    metafields: TranslationMapping[];
  };
  imageMetafieldMappings: {
    groups: ImageGroupMapping[];
  };
}

export interface GraphqlPageInfo {
  hasNextPage: boolean;
  endCursor?: string | null;
}

export interface ProductListFilters {
  query?: string;
  status?: string;
  vendor?: string;
  productType?: string;
  tag?: string;
  focus?: "all" | "missingArabic" | "outdatedArabic" | "imageMismatch" | "missingArabicMedia";
  sort?: "updated" | "title" | "arabicReadiness" | "mediaMismatch";
  localeView?: "english" | "arabic";
}

export interface CursorState {
  after?: string;
  history: string[];
}

export interface ProductImageItem {
  id: string;
  url: string;
  alt?: string | null;
  width?: number | null;
  height?: number | null;
  createdAt?: string | null;
  source?: "product" | "files" | "upload" | "reference";
}

export type ReferenceResourceKind = "MEDIA_IMAGE" | "PRODUCT" | "COLLECTION" | "METAOBJECT";

export interface MetaobjectReferenceField {
  key: string;
  label: string;
  type: MetafieldValueType | string;
  value: string;
  arabicValue: string;
  digest?: string;
  required?: boolean;
  outdated?: boolean;
  isTranslatable: boolean;
}

export interface ReferenceSummary {
  id: string;
  kind: ReferenceResourceKind;
  title: string;
  localizedTitle?: string | null;
  subtitle?: string | null;
  localizedSubtitle?: string | null;
  handle?: string | null;
  image?: ProductImageItem | null;
  metaobject?: {
    definitionId?: string;
    definitionName?: string;
    displayNameKey?: string | null;
    translatable: boolean;
    fields: MetaobjectReferenceField[];
  };
}

export interface MetaobjectDefinitionOption {
  id: string;
  type: string;
  name: string;
  displayNameKey?: string | null;
  translatable?: boolean;
  fieldDefinitions?: Array<{
    key: string;
    name: string;
    required?: boolean;
    type: string;
  }>;
}

export interface ProductIndexItem {
  id: string;
  legacyId: string;
  title: string;
  handle: string;
  status: string;
  vendor: string;
  productType: string;
  tags: string[];
  updatedAt: string;
  thumbnail?: ProductImageItem | null;
  translationStatus: {
    hasArabic: boolean;
    translatedKeys: number;
    totalKeys: number;
    outdatedKeys: number;
  };
  localeImageStatus: {
    hasArabic: boolean;
    englishCount: number;
    arabicCount: number;
    mismatch: boolean;
    delta: number;
    sources: string[];
  };
}

export interface ProductIndexResult {
  items: ProductIndexItem[];
  pageInfo: GraphqlPageInfo;
  cursorState: CursorState;
  filters: ProductListFilters;
}

export interface EditorTranslatableField {
  resourceId: string;
  label: string;
  key: string;
  translationKey: string;
  group: string;
  fieldType: MetafieldValueType | "product_field";
  sourceValue: string;
  arabicValue: string;
  digest: string;
  outdated?: boolean;
}

export interface EditorMetafield {
  id: string;
  namespace: string;
  key: string;
  type: MetafieldValueType | string;
  name: string;
  description?: string | null;
  value: string;
  compareDigest?: string | null;
  referenceIds: string[];
  references: ReferenceSummary[];
  isPinned: boolean;
  isPopulated: boolean;
  translation?: {
    label: string;
    arabicValue: string;
    digest: string;
    fieldType: MetafieldValueType | "product_field";
    outdated?: boolean;
  };
  validations?: Array<{
    name: string;
    value: string;
  }>;
  allowedMetaobjectTypes?: string[];
}

export interface EditorImageTarget {
  target: ImageStorageTarget;
  resourceId?: string;
  digest?: string;
  compareDigest?: string | null;
  images: ProductImageItem[];
  rawValue: string;
}

export interface EditorImageGroup {
  id: string;
  label: string;
  notes: string[];
  english: EditorImageTarget[];
  arabic: EditorImageTarget[];
  legacy: EditorImageTarget[];
  mismatchWarning?: string;
}

export interface ProductEditorModel {
  product: {
    id: string;
    legacyId: string;
    title: string;
    handle: string;
    descriptionHtml: string;
    vendor: string;
    productType: string;
    tags: string[];
    status: string;
    seo: {
      title: string;
      description: string;
    };
    options: Array<{
      id: string;
      name: string;
      values: string[];
    }>;
    media: ProductImageItem[];
  };
  arabicFields: EditorTranslatableField[];
  metafields: EditorMetafield[];
  imageGroups: EditorImageGroup[];
  metaobjectDefinitions: MetaobjectDefinitionOption[];
  raw: {
    productTranslatableKeys: string[];
    metafieldDefinitions: MetafieldDefinitionSummary[];
    translationResourceIds: string[];
    lastDiscoveredAt: string;
  };
}

export type SaveSectionKey = "core" | "arabic" | "metafields" | "images";

export interface SaveSectionResult {
  key: SaveSectionKey;
  ok: boolean;
  message: string;
  errorCount: number;
}

export interface SaveConflict {
  kind: "translation" | "metafield" | "image";
  section: SaveSectionKey;
  resourceId: string;
  fieldKey: string;
  label: string;
  message: string;
  latestValue: string;
  latestDigest?: string;
  latestCompareDigest?: string | null;
  latestSourceValue?: string;
}

export interface SaveResult {
  ok: boolean;
  message: string;
  errors?: Array<{
    field?: string;
    message: string;
  }>;
  sectionResults?: SaveSectionResult[];
  conflicts?: SaveConflict[];
}

export interface BulkOperationRequest {
  productIds: string[];
  operation:
    | "bulkCopyEnglishImagesToArabic"
    | "bulkClearArabicImages"
    | "bulkSetMetafield"
    | "bulkApplyArabicTranslations"
    | "bulkImportJson";
  payload?: Record<string, unknown>;
}
