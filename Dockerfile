# ponytail: Debian (glibc 2.41) instead of alpine+gcompat; native addons need real glibc >= 2.38
FROM node:22-trixie-slim

ENV FFMPEG_PATH=/usr/bin/ffmpeg

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

RUN node -e "import('./src/media.js').then(m => m.ensureDummyMedia())"

CMD ["npm", "start"]
