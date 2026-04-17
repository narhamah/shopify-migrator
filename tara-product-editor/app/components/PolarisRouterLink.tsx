import { Link as RouterLink } from "react-router";
import type { LinkLikeComponent, LinkLikeComponentProps } from "@shopify/polaris/build/ts/src/utilities/link";

export const PolarisRouterLink: LinkLikeComponent = ({
  url,
  external,
  target,
  rel,
  children,
  ...rest
}: LinkLikeComponentProps) => {
  const isExternalUrl = /^https?:\/\//i.test(url) || external;

  if (isExternalUrl) {
    return (
      <a
        {...rest}
        href={url}
        target={target ?? "_blank"}
        rel={rel ?? "noopener noreferrer"}
      >
        {children}
      </a>
    );
  }

  return (
    <RouterLink
      {...rest}
      to={url}
      target={target}
      rel={rel}
    >
      {children}
    </RouterLink>
  );
};
