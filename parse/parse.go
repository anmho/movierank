package parse

import (
	"encoding/csv"
	"os"
)

func ratingsToSQL() {
	file, err := os.Open("./data/rating.csv")
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err == io.EOF {
		fmt.Println("Empty file")
		return
	} else if err != nil {
		fmt.Println("Error reading header:", err)
		return
	}

	fmt.Println("Headers:", headers) // Optional: Print headers

	usersFile, err := os.OpenFile("./db/temp/1-users.sql", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	ratingsFile, err := os.OpenFile("./db/temp/5-ratings.sql", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	i := 0
	userIds := map[int64]bool{}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading record:", err)
			continue
		}

		// Access data using record index or map the header to the corresponding value
		//for i, field := range record {
		//	fmt.Printf("%s: %s\n", headers[i], field) // Access using index
		//}

		// Alternatively, create a map for better readability
		data := make(map[string]string)
		for i, field := range record {
			data[headers[i]] = field
		}
		userId, err := strconv.ParseInt(data["userId"], 10, 64)
		if err != nil {
			log.Println(err)
			continue
		}
		movieId, err := strconv.ParseInt(data["movieId"], 10, 64)
		if err != nil {
			log.Println(err)
			continue
		}
		rating, err := strconv.ParseFloat(data["rating"], 64)
		if err != nil {
			log.Println(err)
			continue
		}
		timestamp := data["timestamp"]

		userStmt := fmt.Sprintf(
			"INSERT INTO users (id) VALUES (%d);\n", userId,
		)
		if !userIds[userId] {
			userIds[userId] = true

			usersFile.Write([]byte(userStmt))
		}

		ratingStmt := fmt.Sprintf(
			"INSERT INTO ratings (user_id, movie_id, rating, rated_at) VALUES (%d, %d, %.5f, '%s');\n", userId, movieId, rating, timestamp,
		)
		_, err = ratingsFile.Write([]byte(ratingStmt))
		if err != nil {
			log.Println(err)
		}

		if i%100_000 == 0 {
			fmt.Printf("%d %s", i, ratingStmt) // Access data using header as key
			fmt.Println(i, userStmt)           // Access data using header as key
			fmt.Println()
		}
		i++
	}
}
