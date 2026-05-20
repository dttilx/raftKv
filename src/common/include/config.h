#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = false;

const int debugMul = 1;
const int HeartBeatTimeout = 25 * debugMul;  // ms
const int ApplyInterval = 10 * debugMul;     // ms

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms
const int RPC_CALL_TIMEOUT = 300 * debugMul;   // ms

#endif  // CONFIG_H
