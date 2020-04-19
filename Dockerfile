FROM node:10-alpine
ENV NODE_ENV=production
COPY . .
RUN npm ci --production
EXPOSE 1833
# run as non-root user, see https://github.com/nodejs/docker-node/blob/master/docs/BestPractices.md#non-root-user
# as the whole app directory is owned by root, this means we can't create or modify files anywhere except in /tmp
USER node
CMD node index.js
