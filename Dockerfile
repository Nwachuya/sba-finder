FROM apify/actor-node:20

WORKDIR /usr/src/app

# Install only production dependencies.
COPY package*.json ./
RUN npm --quiet set progress=false \
    && npm install --omit=dev

# Copy project sources.
COPY . ./

# Run the actor via the npm start script.
CMD ["npm", "start"]
