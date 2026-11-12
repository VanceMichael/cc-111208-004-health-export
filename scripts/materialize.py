from pathlib import Path
import base64


def main():
    for path in Path('fixtures').glob('*.b64'):
        destination = path.with_suffix('')
        value = base64.b64decode(path.read_bytes(), validate=False)
        if not destination.exists() or destination.read_bytes() != value:
            destination.write_bytes(value)


if __name__ == '__main__':
    main()
