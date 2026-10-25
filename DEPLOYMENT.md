# Deployment

The browser game can be hosted by Firebase Hosting or Vercel. The Node backend
runs as a separate Render web service because online play uses persistent
WebSockets. Firebase is used for account storage; race gameplay remains
peer-to-peer.

## Render backend

1. Create a Render Blueprint from this repository using `render.yaml`.
2. Set `FIREBASE_SERVICE_ACCOUNT_JSON` to the Firebase service-account JSON
   value in Render's secret environment settings. Never put it in frontend
   files or Firebase Hosting.
3. Set `ADMIN_PLAYER_ID` to the owner player's ID.
4. Keep the generated service URL consistent with the URL in
   `backend-config.js`. The configured URL is
   `https://my-game-0hzz.onrender.com`.
5. `render.yaml` allows the current Firebase origins and the Vercel URLs
   `https://my-game-topaz-kappa.vercel.app` and
   `https://my-game-afwyondcj-emoji7.vercel.app`. Add any other exact HTTPS
   origins (including custom domains) to Render's `FRONTEND_ORIGINS`
   environment variable. Keep the existing Firebase origins if you still use
   Firebase Hosting. Separate multiple origins with commas; do not include
   paths or trailing slashes.
6. In the Render dashboard, confirm the running service's `FRONTEND_ORIGINS`
   environment variable contains the exact Vercel production origin. A change
   to `render.yaml` does not take effect on an already-running service until
   the Blueprint changes are applied and the service redeploys. Check
   `https://my-game-0hzz.onrender.com/healthz` returns `{"status":"ok"}`.

Render must use HTTPS and keep WebSockets enabled. The free plan can sleep
after inactivity; use an always-on plan if online rooms must be available
without a wake-up delay.

## Vercel frontend

Import the repository into Vercel and deploy with the default project root.
`vercel.json` runs `npm run build:vercel`, which copies only `index.html`,
`backend-config.js`, and `404.html` into the static output directory. This
keeps the server code and Firebase service-account file out of the frontend
deployment. The frontend uses the Render URL in `backend-config.js`; update
that URL if your Render service has a different hostname.

If your Vercel production URL changes, update its origin in Render's
`FRONTEND_ORIGINS` and redeploy or restart the Render service. Preview
deployments have different origins; add them only if you need account access
from previews.
If visitors are redirected to a Vercel login page, turn off Deployment
Protection for the production deployment in the Vercel project settings.

## Firebase Hosting

`backend-config.js` is public browser configuration and contains only the
backend URL. `firebase.json` excludes the backend source, local data, and
credential file from the Hosting upload. Deploy the frontend with:

```sh
firebase deploy --only hosting
```

The browser sends account requests to the configured backend over HTTPS and
connects to its `/multiplayer` endpoint using WSS. Do not add Firebase Admin
credentials to the frontend or Hosting configuration.
