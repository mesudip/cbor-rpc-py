import sys


def main():
    print("stdio_test_script: Started.", file=sys.stderr)

    while True:
        print("stdio_test_script: Blocked on read", file=sys.stderr)
        data = sys.stdin.read(1024)  # Read up to 1024 bytes
        print(f"stdio_test_script: Read {len(data)} bytes from stdin.", file=sys.stderr)
        sys.stderr.flush()
        if not data:  # EOF reached
            print("stdio_test_script: EOF reached, exiting.", file=sys.stderr)
            sys.stderr.flush()
            break
        sys.stdout.write(data)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
