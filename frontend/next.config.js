/** @type {import('next').NextConfig} */
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
};

module.exports = nextConfig;
