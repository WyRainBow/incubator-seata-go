/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flagext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDayValueYAML(t *testing.T) {
	// Test embedding of DayValue.
	{
		type TestStruct struct {
			Day DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		require.NoError(t, testStruct.Day.Set("1985-06-02"))
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}

	// Test pointers of DayValue.
	{
		type TestStruct struct {
			Day *DayValue `yaml:"day"`
		}

		var testStruct TestStruct
		testStruct.Day = &DayValue{}
		require.NoError(t, testStruct.Day.Set("1985-06-02"))
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
	// Test UTC-stable string and YAML serialization in western timezones.
	// DayValue serializes through .UTC() and parses date-only strings as UTC, so
	// the result is independent of the process local timezone. We therefore do NOT
	// mutate the global time.Local here: doing so races with the Go runtime's
	// background timer goroutine (which reads time.Local via time.Now()) under
	// -race, and has no effect on the code under test anyway.
	{
		type TestStruct struct {
			Day *DayValue `yaml:"day"`
		}
		var testStruct TestStruct
		testStruct.Day = &DayValue{}
		require.NoError(t, testStruct.Day.Set("1985-06-02"))
		expected := []byte(`day: "1985-06-02"
`)

		actual, err := yaml.Marshal(testStruct)
		require.NoError(t, err)
		assert.Equal(t, expected, actual)

		var actualStruct TestStruct
		err = yaml.Unmarshal(expected, &actualStruct)
		require.NoError(t, err)
		assert.Equal(t, testStruct, actualStruct)
	}
}
