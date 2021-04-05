import argparse
import datetime
import itertools as it
import pickle
from operator import itemgetter
import os.path

import arrow
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


def work_events_between(service, calendar_id, period_start, period_end):
    events_result = (
        service.events()
        .list(
            calendarId=calendar_id,
            timeMin=period_start.isoformat(),
            timeMax=period_end.isoformat(),
            maxResults=100,
            singleEvents=True,
            orderBy="startTime",
        )
        .execute()
    )
    rawevents = events_result.get("items", [])

    if not rawevents:
        print("No events found.")

    events_simple = (
        {
            "client": rawevent["summary"],
            "start": arrow.get(rawevent["start"]["dateTime"]),
            "end": arrow.get(rawevent["end"]["dateTime"]),
        }
        for rawevent in rawevents
    )

    events_with_truncated = (
        {
            "client": event["client"],
            "start": event["start"],
            "end": event["end"],
            "truncated end": min(period_end, event["end"]),
        }
        for event in events_simple
    )

    ewt1, ewt2 = it.tee(events_with_truncated)

    events = (
        dict(
            **event,
            **{
                "duration in hours": (
                    event["truncated end"] - event["start"]
                ).total_seconds()
                / 3600,
            },
        )
        for event in ewt1
    )

    has_truncates = any(event["end"] != event["truncated end"] for event in ewt2)

    return list(events), has_truncates


def aggregate_by_client(events_list):
    selector = itemgetter("client")
    return {
        client: sum(event["duration in hours"] for event in group)
        for client, group in it.groupby(sorted(events_list, key=selector), selector)
    }


def main():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    service = build("calendar", "v3", credentials=creds)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--calendar-id", help="Google Calendar calendar ID")
    parser.add_argument("-s", "--start", help="start date (start of week by default)")
    parser.add_argument("-e", "--end", help="end date (end of week by default)")
    parser.add_argument(
        "-z", "--timezone", help="Time zone (Paris by default)", default="Europe/Paris"
    )
    args = parser.parse_args()

    calendar_id = args.calendar_id

    now = arrow.now(tz=args.timezone)

    if args.start is not None:
        start = arrow.get(
            datetime.datetime.strptime(args.start, "%Y-%m-%d"), args.timezone
        )
    else:
        start = arrow.get(now.date() - datetime.timedelta(now.weekday()), args.timezone)
    if args.end is not None:
        end = arrow.get(datetime.datetime.strptime(args.end, "%Y-%m-%d"), args.timezone)
    else:
        end = arrow.get(
            now.date() + datetime.timedelta(8 - now.weekday()), args.timezone
        )

    events_list, has_truncates = work_events_between(service, calendar_id, start, end)
    aggregates = aggregate_by_client(events_list)

    start_str = start.format("YYYY-MM-DD HH:mm ZZZ")
    end_str = end.format("YYYY-MM-DD HH:mm ZZZ")
    print(f"Aggregates between {start_str} and {end_str}:")
    if has_truncates:
        print(
            "⚠️  Some events are ending after the requested period. Overflowing hours are not counted."
        )
    for client, hours in aggregates.items():
        print(f"{client}:\t{str(hours).rjust(5)} hours")


if __name__ == "__main__":
    main()
