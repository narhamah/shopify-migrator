import { Box, Text } from "@shopify/polaris";

export function JsonCodeBlock({
  value,
  height = 280,
}: {
  value: string;
  height?: number;
}) {
  return (
    <Box background="bg-surface-secondary" padding="300" borderRadius="200" overflowX="scroll">
      <pre style={{ minHeight: `${height}px`, margin: 0 }}>
        <Text as="span" variant="bodySm" tone="subdued">
          {value}
        </Text>
      </pre>
    </Box>
  );
}
