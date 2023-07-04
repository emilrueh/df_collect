def check_existing_records(headers, get_all_endpoint_url):
    time.sleep(api_rate_limit_wait())

    response = requests.get(get_all_endpoint_url, headers=headers)
    if response.status_code != 200:
        print(colored("Failed to retrieve data from the database.", "yellow"))
        raise Exception(
            f"Request failed with status {response.status_code}. Message: {response.text}"
        )
    existing_entries = response.json()

    return {
        entry["Link"]: {
            "id": entry["id"],
            "entry": entry,
            "Highlights": entry.get("Highlights"),
            "bookmark_users_id": entry.get("bookmark_users_id"),
        }
        for entry in existing_entries
    }


def send_request_with_retry(url, method, headers, data=None, max_attempts=3):
    time.sleep(api_rate_limit_wait())

    for attempt in range(max_attempts):
        try:
            if method == "POST":
                response = requests.post(url, headers=headers, data=data)
            elif method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, data=data)

            response.raise_for_status()
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.HTTPError,
            Exception,
        ) as e:
            wait_time = 2**attempt
            print(
                f"\n{colored(f'Attempt {attempt+1}', 'light_red')} failed with error: {e}.\nRetrying in {wait_time} seconds...",
            )
            time.sleep(wait_time)
            if attempt == max_attempts - 1:
                print(colored("\nALL FAILED!", color="light_red", attrs=["bold"]))
                try:
                    print(f"Response body: {response.text}")
                except UnboundLocalError:
                    print("Response object is not available.")
                raise
        else:
            return response
    return None  # This should never be reached but is included for completeness


def update_entry_and_print(
    transformed_row, headers, edit_url, current_event_id, row_index
):
    response = send_request_with_retry(
        edit_url, "POST", headers, json.dumps(transformed_row)
    )
    print(
        row_index,
        colored("UPDATED", color="light_yellow", attrs=["bold"]),
        f"ID {colored(current_event_id, color='magenta')} {transformed_row['Link']}",
    )
    response_json = response.json()
    return response_json


def skip_entry_and_print(row_index, current_event_id, link):
    if current_event_id == None:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"as past {link}",
        )
    else:
        print(
            row_index,
            colored("SKIPPED", color="light_green", attrs=["bold"]),
            f"ID {colored(current_event_id, 'magenta')} {link}",
        )


def send_data_to_xano(
    data,
    api_key,
    image_endpoint_url,
    send_endpoint_url,
    check_endpoint_url,
    edit_endpoint_url,
    delete_endpoint_url,
    table_name,
):
    if isinstance(data, pd.DataFrame):
        data = data.replace({np.nan: None})
        data = data.to_dict(orient="records")

    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    response_list = []

    print(colored(f"Accessing Xano data base {table_name}...\n", "cyan"))
    table_name = table_name.replace(" ", "_").lower()

    existing_entries = check_existing_records(
        headers=headers, get_all_endpoint_url=check_endpoint_url
    )

    today = datetime.datetime.now().date()  # get the date part of the current datetime

    for i, row in enumerate(data):
        row_index = i + 1
        row_index = f"{row_index:4}"

        transformed_row = transform_data(row)
        link = transformed_row["Link"]

        # Process image metadata
        if "Photo" in transformed_row and transformed_row["Photo"]:
            base64_image = transformed_row["Photo"]
            response = send_request_with_retry(
                image_endpoint_url,
                "POST",
                headers,
                json.dumps({"content": base64_image}),
            )

            image_metadata = response.json()
            transformed_row["Photo"] = [
                {
                    "path": image_metadata["path"],
                    "name": image_metadata["name"],
                    "type": image_metadata["type"],
                    "size": image_metadata["size"],
                    "mime": image_metadata["mime"],
                    "meta": image_metadata["meta"],
                }
            ]

        # check if the current event is in the existing entries and if so, set current_event_id
        current_event_id = (
            existing_entries[link]["id"] if link in existing_entries else None
        )
        colored_event_id = colored(current_event_id, "magenta")

        # Skip events with a date in the past
        if "Date" in transformed_row:
            event_date_str = transformed_row["Date"].split("T")[0]
            event_date = datetime.datetime.strptime(
                event_date_str, "%Y-%m-%d"
            ).date()  # convert the date string to a date object

            if event_date < today:
                skip_entry_and_print(
                    row_index, current_event_id, transformed_row["Link"]
                )
                continue  # move to the next row in data if this event is in the past

        if (
            link in existing_entries
        ):  # If link is in existing_entries, check date for archive/delete
            event_date_str = existing_entries[link]["entry"]["Date"].split("T")[0]
            event_date = datetime.datetime.strptime(
                event_date_str, "%Y-%m-%d"
            ).date()  # convert the date string to a date object

            # Updating archived or deleting old events
            if event_date < today:
                if existing_entries[link][
                    "bookmark_users_id"
                ]:  # Checks for truthy value
                    transformed_row["Archived"] = True
                    edit_url = edit_endpoint_url.format(id=current_event_id)
                    response_json = update_entry_and_print(
                        transformed_row, headers, edit_url, current_event_id, row_index
                    )
                    response_list.append(response_json)
                    continue
                else:
                    # Delete the entry
                    if current_event_id:
                        delete_url = delete_endpoint_url.format(id=current_event_id)
                        delete_param = {f"{table_name}_id": int(current_event_id)}
                        response = send_request_with_retry(
                            delete_url,
                            "DELETE",
                            headers,
                            data=json.dumps(delete_param),
                        )
                        print(
                            row_index,
                            colored("DELETED", color="light_magenta", attrs=["bold"]),
                            f"ID {colored_event_id} {transformed_row['Link']}",
                        )
                        continue

        if current_event_id:
            if entries_are_equal(
                {
                    k: v
                    for k, v in transformed_row.items()
                    if k not in ["bookmark_users_id", "Highlights"]
                },
                {
                    k: v
                    for k, v in existing_entries[link]["entry"].items()
                    if k not in ["bookmark_users_id", "Highlights"]
                },
            ):
                skip_entry_and_print(
                    row_index, current_event_id, transformed_row["Link"]
                )
                continue
            else:
                edit_url = edit_endpoint_url.format(id=current_event_id)

                if existing_entries[link]["bookmark_users_id"]:
                    transformed_row["bookmark_users_id"] = existing_entries[link][
                        "bookmark_users_id"
                    ]

                if existing_entries[link]["Highlights"]:
                    transformed_row["Highlights"] = existing_entries[link]["Highlights"]

                transformed_row[f"{table_name}_id"] = current_event_id

                response_json = update_entry_and_print(
                    transformed_row, headers, edit_url, current_event_id, row_index
                )

                response_list.append(response_json)
                continue

        row_json = json.dumps(transformed_row)
        headers.update({"Content-Type": "application/json"})

        response = send_request_with_retry(send_endpoint_url, "POST", headers, row_json)

        if response.status_code == 200:
            current_event_id = response.json()["id"]
            print(
                row_index,
                colored("CREATED", color="light_blue", attrs=["bold"]),
                f"ID {colored_event_id} {transformed_row['Link']}",
            )

        response_json = response.json()
        response_list.append(response_json)

    return response_list
