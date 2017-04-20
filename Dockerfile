FROM php:7.0

RUN apt-get update && apt-get install unzip git libxml2-dev -y

WORKDIR /code

# Initialize
COPY . /code/

# install composer
RUN curl -sS https://getcomposer.org/installer | php \
  && mv /code/composer.phar /usr/local/bin/composer \
  && composer install

CMD php /code/run.php --data=/data