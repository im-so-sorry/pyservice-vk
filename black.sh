#!/usr/bin/env bash
FUNCTION=
if [ ! -z $1 ]; then
  FUNCTION="$1"
fi

SOURCE_ROOT="./vk_service"


check() {
  black --config pyproject.toml --check $SOURCE_ROOT
}

diff() {
  black --config pyproject.toml --diff $SOURCE_ROOT
}

format() {
  black --config pyproject.toml $SOURCE_ROOT
}

case "$1" in
-h | --help)
  show-help
  ;;
*)
  if [ ! -z $(type -t $FUNCTION | grep function) ]; then
    $1 $2 $3 $4 $5
  else
    format
  fi
  ;;
esac
