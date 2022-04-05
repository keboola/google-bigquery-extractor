FROM php:7.2

RUN apt-get update && apt-get install unzip git libxml2-dev -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /code

# Initialize
COPY . /code/

# install composer
RUN curl -sS https://getcomposer.org/installer | php \
  && mv /code/composer.phar /usr/local/bin/composer \
  && composer install

CMD php /code/src/run.php --data=/data
