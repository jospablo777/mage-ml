"""
HTTP smoke tests for the Tornado app.

test_server.py builds the app and inspects its router without serving anything,
so a dependency bump can break the UI while it stays green. This module starts
the app on a real port and issues real requests.
"""
import tornado.testing

from mage_ai.server.server import make_app

# SPA routes that render the same document. Broken template loading or routing
# shows up on all of them.
PAGE_ROUTES = [
    '/',
    '/pipelines',
    '/settings',
    '/terminal',
    '/triggers',
    '/templates',
]


class ServerHTTPSmokeTest(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        return make_app()

    def test_main_page_is_served(self):
        response = self.fetch('/')

        self.assertEqual(response.code, 200)
        self.assertIn('text/html', response.headers.get('Content-Type', ''))

        body = response.body.decode('utf-8', errors='replace')
        # The frontend is a Next.js export, so its markup carries __next.
        self.assertIn('<html', body.lower())
        self.assertIn('__next', body)

    def test_spa_routes_all_render(self):
        for route in PAGE_ROUTES:
            with self.subTest(route=route):
                response = self.fetch(route)
                self.assertEqual(response.code, 200)
                self.assertIn('<html', response.body.decode('utf-8', errors='replace').lower())

    def test_unknown_route_is_not_served(self):
        response = self.fetch('/this-route-does-not-exist')

        self.assertEqual(response.code, 404)

    def test_static_asset_route_is_registered(self):
        response = self.fetch('/_next/static/this-file-does-not-exist.js')

        # 404 means the static handler is mounted. A 500 means the route or the
        # asset directory is gone, which leaves the UI without its bundle.
        self.assertEqual(response.code, 404)


if __name__ == '__main__':
    tornado.testing.main()
