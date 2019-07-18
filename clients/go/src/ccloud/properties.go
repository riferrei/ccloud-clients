package ccloud

import (
	"bufio"
	"log"
	"os"
	"strings"
)

const propsFile string = "../../../../resources/ccloud.properties"

// LoadProperties loads the ccloud.properties file into the map passed
// as reference so the main apps can create producers and consumers
func LoadProperties(props map[string]string) {
	file, err := os.Open(propsFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 {
			if !strings.HasPrefix(line, "//") &&
				!strings.HasPrefix(line, "#") {
				if strings.HasPrefix(line, "sasl.jaas.config") {
					parts := strings.Split(line, "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required ")
					saslJaasConfig := strings.ReplaceAll(parts[1], ";", "")
					userPassHolder := strings.Split(saslJaasConfig, " ")
					userName := strings.Split(userPassHolder[0], "=")
					props["sasl.username"] = strings.ReplaceAll(userName[1], "\"", "")
					password := strings.Split(userPassHolder[1], "=")
					props["sasl.password"] = strings.ReplaceAll(password[1], "\"", "")
				} else if strings.HasPrefix(line, "schema.registry.basic.auth.user.info") {
					parts := strings.Split(line, "=")
					userPassHolder := strings.Split(parts[1], ":")
					props["schema.registry.basic.auth.username"] = strings.TrimSpace(userPassHolder[0])
					props["schema.registry.basic.auth.password"] = strings.TrimSpace(userPassHolder[1])
				} else {
					parts := strings.Split(line, "=")
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					props[key] = value
				}
			}
		}
	}

}
