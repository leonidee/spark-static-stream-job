import time


def task1():
    for i in range(100):
        print(f"This is {i}")
        time.sleep(1)


def task2():
    for i in range(3):
        print(f"Hello from task2. Run {i}")
        time.sleep(10)

    print("I'am off. Goodbye!")


# def main():
#     import threading

#     thread1 = threading.Thread(target=task1)
#     thread2 = threading.Thread(target=task2)

#     thread1.start()
#     thread2.start()

#     thread1.join()
#     thread2.join()


def main():
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor() as executor:
        f1 = executor.submit(task1)
        f2 = executor.submit(task2)


if __name__ == "__main__":
    main()
