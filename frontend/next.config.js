/** @type {import('next').NextConfig} */
const backendApiOrigin = (process.env.BACKEND_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/+$/, "");

const nextConfig = {
  allowedDevOrigins: ["127.0.0.1", "localhost"],
  experimental: {
    devtoolSegmentExplorer: false,
  },
  async redirects() {
    return [
      {
        source: "/exposures",
        destination: "/cuse/exposures",
        permanent: false,
      },
      {
        source: "/explore",
        destination: "/cuse/explore",
        permanent: false,
      },
      {
        source: "/health",
        destination: "/cuse/health",
        permanent: false,
      },
      {
        source: "/cuse",
        destination: "/cuse/exposures",
        permanent: false,
      },
      {
        source: "/cpar",
        destination: "/cpar/risk",
        permanent: false,
      },
      {
        source: "/cpar/portfolio",
        destination: "/cpar/risk",
        permanent: false,
      },
    ];
  },
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${backendApiOrigin}/api/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
