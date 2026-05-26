import dotenv
import babamul

dotenv.load_dotenv()

topics = ["^babamul.*"]
with babamul.AlertConsumer(
    topics=topics,
    offset="earliest",
    auto_commit=False,
    timeout=15,
) as consumer:
    for alert in consumer:
        alert.show()
        break
