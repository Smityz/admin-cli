/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package executor

import (
	"errors"
	"os"
)

type strategyMode int32

const (
	ver1ToVer1 strategyMode = 0
	ver1ToVer2 strategyMode = 1
	ver2ToVer1 strategyMode = 2
	ver2ToVer2 strategyMode = 3
	unKnown    strategyMode = 4
)

type tableInfo struct {
	tableName      string
	partitionCount int32
	meta           []string
	wReqRateLimit  string
	client         *Client
}

type migrationInfo struct {
	source *tableInfo
	target *tableInfo
}

func generateCopyDataStrategy() migrationStep {
	strategy := &startMigration{}

	createTableIfNotExsit := &createTableIfNotExsit{}
	stopSourceTableWrite := &stopSourceTableWrite{}
	copyData := &copyData{}
	notifyMetoProxy := &notifyMetoProxy{}

	strategy.next = createTableIfNotExsit
	createTableIfNotExsit.next = stopSourceTableWrite
	stopSourceTableWrite.next = copyData
	copyData.next = notifyMetoProxy

	return strategy
}

// +----------+         6
// |  Meta    <----------------------+
// |  Proxy   |                      |
// +----^-----+                 +-----------+
//      |                 0     |  Pegasus  |   6
//      |              +--------+  Client   +--------+
//      |5             |        +-----------+        |
//      |              |                             |
//      |         +----v--------+           +--------v----+
//      |         |             |           |             |
// +----------+ 4 |   Origin    |    3      |   Target    |
// | Mig-tool +--->   Table     +----------->   Table     |
// +----------+   |             |           |             |
//                +----+--------+           +--------^----+
//                     |                             |
//                     |1   +--------------------+   |2
//                     +----> Cold Backup Server +---+
//                          +--------------------+

func generateHotMigrationStrategy() migrationStep {
	strategy := &startMigration{}

	createTableIfNotExsit := &createTableIfNotExsit{}
	backUpSourceTable := &backUpSourceTable{}
	recoverBackupOnTargetTable := &recoverBackupOnTargetTable{}
	startDuplication := &startDuplication{}
	observeProgress := &observeProgress{}
	stopSourceTableWrite := &stopSourceTableWrite{}
	waitDuplicationFinish := &waitDuplicationFinish{}
	notifyMetoProxy := &notifyMetoProxy{}

	strategy.next = createTableIfNotExsit
	createTableIfNotExsit.next = backUpSourceTable
	backUpSourceTable.next = recoverBackupOnTargetTable
	recoverBackupOnTargetTable.next = startDuplication
	startDuplication.next = waitDuplicationFinish
	stopSourceTableWrite.next = observeProgress

	observeProgress.next = notifyMetoProxy

	return strategy
}

func MigrateTable(tableName string, sourceMeta []string, targetMeta []string, mode strategyMode) error {

	i := &migrationInfo{
		source: &tableInfo{
			tableName: tableName,
			meta:      sourceMeta,
			client:    NewClient(os.Stdout, sourceMeta),
		},
		target: &tableInfo{
			tableName: tableName,
			meta:      targetMeta,
			client:    NewClient(os.Stdout, targetMeta),
		},
	}
	var strategy migrationStep

	if mode == unKnown {
		mode = checkServerVersion(i)
	}

	switch mode {
	case ver1ToVer1:
	case ver1ToVer2:
	case ver2ToVer1:
		strategy = generateCopyDataStrategy()
	case ver2ToVer2:
		strategy = generateHotMigrationStrategy()
	}

	return strategy.execute(i)
}

func checkServerVersion(i *migrationInfo) strategyMode {
	return unKnown
}

type migrationStep interface {
	execute(*migrationInfo) error
	doNext(*migrationInfo, error) error
}

type startMigration struct {
	next migrationStep
}

func (this *startMigration) execute(i *migrationInfo) error {
	var err error
	return this.doNext(i, err)
}

func (this *startMigration) doNext(i *migrationInfo, err error) error {
	return this.next.execute(i)
}

type createTableIfNotExsit struct {
	next migrationStep
}

func (this *createTableIfNotExsit) execute(i *migrationInfo) error {
	var err error

	list, err := listTables(i.source.client, false)
	if err != nil {
		return this.doNext(i, err)
	}

	for _, table := range list {
		tableinfo, ok := table.(availableTableStruct)
		if !ok {
			return this.doNext(i, errors.New("Can't convert table interface to tableinfo"))
		}
		if tableinfo.Name == i.source.tableName {
			i.target.tableName = i.source.tableName
			i.target.partitionCount = tableinfo.PartitionCount
			i.target.wReqRateLimit = tableinfo.WReqRateLimit
			break
		}
	}

	if err = CreateTable(i.target.client, i.target.tableName, int(i.target.partitionCount), 3); err != nil {
		return this.doNext(i, err)
	}

	return this.doNext(i, nil)
}

func (this *createTableIfNotExsit) doNext(i *migrationInfo, err error) error {
	if err != nil {
		return err
	}
	return this.next.execute(i)
}

type stopSourceTableWrite struct {
	next                migrationStep
	originWReqRateLimit string
}

func (this *stopSourceTableWrite) execute(i *migrationInfo) error {
	var err error

	if err = SetAppEnv(i.source.client, i.source.tableName, "WReqRateLimit", "0*delay*100,0*reject*200"); err != nil {
		return this.doNext(i, err)
	}

	return this.doNext(i, nil)
}

func (this *stopSourceTableWrite) doNext(i *migrationInfo, err error) error {
	if err != nil {
		return err
	}
	if err = this.next.execute(i); err != nil {
		// roll back
		_ = SetAppEnv(i.source.client, i.source.tableName, "WReqRateLimit", i.source.tableName)
	}
	return nil
}

type copyData struct {
	next migrationStep
}

func (this *copyData) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *copyData) doNext(i *migrationInfo, err error) error {
}

type notifyMetoProxy struct {
	next migrationStep
}

func (this *notifyMetoProxy) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *notifyMetoProxy) doNext(i *migrationInfo, err error) error {
}

type backUpSourceTable struct {
	next migrationStep
}

func (this *backUpSourceTable) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *backUpSourceTable) doNext(i *migrationInfo, err error) error {
}

type recoverBackupOnTargetTable struct {
	next migrationStep
}

func (this *recoverBackupOnTargetTable) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *recoverBackupOnTargetTable) doNext(i *migrationInfo, err error) error {
}

type startDuplication struct {
	next migrationStep
}

func (this *startDuplication) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *startDuplication) doNext(i *migrationInfo, err error) error {
}

type observeProgress struct {
	next migrationStep
}

func (this *observeProgress) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *observeProgress) doNext(i *migrationInfo, err error) error {
}

type waitDuplicationFinish struct {
	next migrationStep
}

func (this *waitDuplicationFinish) execute(i *migrationInfo) error {
	this.next.execute(i)
}

func (this *waitDuplicationFinish) doNext(i *migrationInfo, err error) error {
}
