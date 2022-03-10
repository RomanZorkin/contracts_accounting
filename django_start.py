import os


def django_start():
    import web.mysite.manage as manage
    manage.main()

if __name__ == "__main__":
    django_start()