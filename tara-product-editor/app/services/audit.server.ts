import prisma from "~/db.server";

export async function writeAuditLog(input: {
  shop: string;
  action: string;
  status: "success" | "error";
  resourceType?: string;
  resourceId?: string;
  payload?: unknown;
  errorMessage?: string;
}) {
  await prisma.auditLog.create({
    data: {
      shop: input.shop,
      action: input.action,
      resourceType: input.resourceType,
      resourceId: input.resourceId,
      status: input.status,
      payloadJson: input.payload ? JSON.stringify(input.payload) : undefined,
      errorMessage: input.errorMessage,
    },
  });
}
