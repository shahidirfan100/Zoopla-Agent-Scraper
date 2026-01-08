# Use Playwright + Camoufox image for maximum stealth
# This base image includes Firefox with Camoufox pre-configured
FROM apify/actor-node-playwright-camoufox:22-1.56.1

# Check preinstalled packages
RUN npm ls @crawlee/core apify playwright || true

# Copy just package.json and package-lock.json
# to speed up the build using Docker layer cache.
COPY --chown=myuser:myuser package*.json Dockerfile ./

# Install NPM packages, skip development dependencies to
# keep the image small. Avoid logging too much and print the dependency
# tree for debugging
RUN npm --quiet set progress=false \
    && npm install --omit=dev \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version \
    && rm -r ~/.npm

# Next, copy the remaining files and directories with the source code.
# Since we do this after NPM install, quick build will be really fast
# for most source file changes.
COPY --chown=myuser:myuser . ./

CMD ["node", "src/main.js"]
