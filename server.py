import asyncio
from datetime import datetime

import config
import hashlib

import aiopg

from aiohttp import web
from gino import Gino
from asyncpg.exceptions import UniqueViolationError


db = Gino()


class BaseModel:

    @classmethod
    async def get_or_404(cls, id):
        instance = await cls.get(id)
        if instance:
            return instance
        raise web.HTTPNotFound()

    @classmethod
    async def delete_or_404(cls, id):
        instance = await cls.get(id)
        if instance:
            await instance.delete()
            return id
        raise web.HTTPNotFound()

    @classmethod
    async def create_instance(cls, **kwargs):
        try:
            instance = await cls.create(**kwargs)
        except UniqueViolationError:
            raise web.HTTPBadRequest()
        return instance


class User(db.Model, BaseModel):

    __tablename__ = 'user'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    password = db.Column(db.String(128))

    def __repr__(self):
        return '<User {}>'.format(self.username)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email
        }

    @classmethod
    async def create_instance(cls, **kwargs):
        kwargs['password'] = hashlib.md5(kwargs['password'].encode()).hexdigest()
        return await super().create_instance(**kwargs)


class Advert(db.Model, BaseModel):

    __tablename__ = 'advert'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(64), index=True)
    text = db.Column(db.String(256))
    timestamp = db.Column(db.String(32), index=True, default=datetime.isoformat(datetime.utcnow()))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    def __repr__(self):
        return '<Advert {} - {}>'.format(self.title, self.text)

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'text': self.text,
            'owner': self.user_id,
            'timestamp': self.timestamp
        }


async def set_connection():
    return await db.set_bind(config.DB_DSN)


async def disconnect():
    return await db.pop_bind().close()


async def pg_pool(app):
    async with aiopg.create_pool(config.DB_DSN) as pool:
        app['pg_pool'] = pool
        yield
        pool.close()


async def orm_engine(app):
    app['db'] = db
    await set_connection()
    await db.gino.create_all()
    yield
    await disconnect()


class HealthView(web.View):

    async def get(self):
        return web.json_response({'status': 'OK!'})


class UserView(web.View):

    async def get(self):
        user_id = int(self.request.match_info['user_id'])
        user = await User.get_or_404(user_id)
        return web.json_response(user.to_dict())

    async def post(self):
        data = await self.request.json()
        user = await User.create_instance(**data)
        return web.json_response(user.to_dict())


class AdvertView(web.View):

    async def get(self):
        advert_id = int(self.request.match_info['advert_id'])
        advert = await Advert.get_or_404(advert_id)
        return web.json_response(advert.to_dict())

    async def delete(self):
        advert_id = int(self.request.match_info['advert_id'])
        advert = await Advert.delete_or_404(advert_id)
        return web.json_response({'Deleted ID': advert})

    async def post(self):
        data = await self.request.json()
        advert = await Advert.create_instance(**data)
        return web.json_response(advert.to_dict())


class Users(web.View):

    async def get(self):
        pool = self.request.app['pg_pool']
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT id, username, email FROM public.user')
                users = await cursor.fetchall()
                return web.json_response(users)


class Adverts(web.View):

    async def get(self):
        pool = self.request.app['pg_pool']
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT id, title, text, user_id, timestamp FROM public.advert')
                adverts = await cursor.fetchall()
                return web.json_response(adverts)


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
app = web.Application()
app.cleanup_ctx.append(orm_engine)
app.cleanup_ctx.append(pg_pool)
app.add_routes([web.get('/', HealthView)])
app.add_routes([web.get(r'/user/{user_id:\d+}', UserView)])
app.add_routes([web.post('/user', UserView)])
app.add_routes([web.get('/users', Users)])
app.add_routes([web.get(r'/advert/{advert_id:\d+}', AdvertView)])
app.add_routes([web.post('/advert', AdvertView)])
app.add_routes([web.get('/adverts', Adverts)])
app.add_routes([web.delete(r'/advert/{advert_id:\d+}', AdvertView)])

web.run_app(app, port=8080)
