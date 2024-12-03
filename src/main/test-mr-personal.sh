#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

# 判断当前OS是不是Mac
if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1

maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      # "$@" 是一个特殊变量，它代表函数或脚本调用时传入的所有参数。
      # /dev/null 2>&1 是将标准输出（stdout）和标准错误（stderr）都重定向到 /dev/null，这意味着所有输出都会被丢弃，不会显示在终端上。
      "$@" > /dev/null 2>&1
    else
      # 直接输出到终端上
      "$@"
    fi
}


# 生成TIMEOUT命令
TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
# 在新的子目录中运行测试
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
# 确保软件是全新构建的。
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
# 这行代码将 mr-out-0 文件中的内容进行排序，并将排序后的结果重定向到 mr-correct-wc.txt 文件中
sort mr-out-0 > mr-correct-wc.txt
# 删除所有以 mr-out 开头的文件。
rm -f mr-out*

echo '***' Starting wc test.

# 执行 ../mrcoordinator ../pg*txt
maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &

# 这行代码将后台命令的进程 ID（PID）赋值给变量 pid。
# $! 是一个特殊变量，它代表最近一个放到后台执行的命令的 PID。
pid=$!

# give the coordinator time to create the sockets.
# 等待coordinator创建sockets
sleep 1

# start multiple workers.
# 启动多个 worker
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &

# wait for the coordinator to exit.
# 等待协调器退出
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
# 任务完成，对比
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
