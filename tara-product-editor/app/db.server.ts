import { PrismaClient } from "@prisma/client";

declare global {
  // eslint-disable-next-line no-var
  var __taraProductEditorPrisma__: PrismaClient | undefined;
}

const prisma =
  global.__taraProductEditorPrisma__ ||
  new PrismaClient({
    log: process.env.NODE_ENV === "development" ? ["error", "warn"] : ["error"],
  });

if (process.env.NODE_ENV !== "production") {
  global.__taraProductEditorPrisma__ = prisma;
}

export default prisma;
