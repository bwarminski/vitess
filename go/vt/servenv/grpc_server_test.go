/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/orca"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	require.Empty(t, interceptors.Build(), "expected empty builder to report as empty")
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	require.Len(t, interceptors.streamInterceptors, 1, "expected 1 server options to be available")
	require.Len(t, interceptors.unaryInterceptors, 1, "expected 1 server options to be available")
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	require.Len(t, interceptors.streamInterceptors, 2, "expected 2 server options to be available")
	require.Len(t, interceptors.unaryInterceptors, 2, "expected 2 server options to be available")
}

func TestOrcaRecorder(t *testing.T) {
	recorder := orca.NewServerMetricsRecorder()

	recorder.SetCPUUtilization(0.25)
	recorder.SetMemoryUtilization(0.5)

	snap := recorder.ServerMetrics()

	assert.Equalf(t, 0.25, snap.CPUUtilization, "expected cpu 0.25, got %v", snap.CPUUtilization)
	assert.Equalf(t, 0.5, snap.MemUtilization, "expected memory 0.5, got %v", snap.MemUtilization)
}

func TestReportedOrca(t *testing.T) {
	// Set the port to enable gRPC server.
	restorePort := withTempVar(&gRPCPort, getFreePort())
	defer restorePort()
	restoreOrcaMetrics := withTempVar(&gRPCEnableOrcaMetrics, true)
	defer restoreOrcaMetrics()
	restoreMetricsRecorder := withTempVar(&GRPCServerMetricsRecorder, nil)
	defer restoreMetricsRecorder()
	restoreGRPCServer := withTempVar(&GRPCServer, (*grpc.Server)(nil))
	defer restoreGRPCServer()

	createGRPCServer()
	assert.NotNil(t, GRPCServerMetricsRecorder, "GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is true")
	GRPCServerMetricsRecorder.SetCPUUtilization(getCpuUsage())
	GRPCServerMetricsRecorder.SetMemoryUtilization(getMemoryUsage())

	serverMetrics := GRPCServerMetricsRecorder.ServerMetrics()
	cpuUsage := serverMetrics.CPUUtilization
	assert.GreaterOrEqualf(t, cpuUsage, float64(0), "CPU Utilization is not set %.2f", cpuUsage)
	t.Logf("CPU Utilization is %.2f", cpuUsage)

	memUsage := serverMetrics.MemUtilization
	assert.GreaterOrEqualf(t, memUsage, float64(0), "Mem Utilization is not set %.2f", memUsage)
	t.Logf("Memory utilization is %.2f", memUsage)
}

// TestGRPCServerSkipsIngressStatsByDefault verifies that servenv gRPC servers
// do not record ingress bytes unless a binary opts in.
func TestGRPCServerSkipsIngressStatsByDefault(t *testing.T) {
	restore := withTempVar(&gRPCIngressStatsEnabled, false)
	defer restore()

	var ingressBytes uint64
	runIngressStatsTestRPC(t, &ingressBytes)

	assert.Zero(t, atomic.LoadUint64(&ingressBytes))
}

// TestEnableGRPCIngressStatsInstallsServerOption verifies that opt-in servers
// attach inbound gRPC payload bytes to RPC contexts.
func TestEnableGRPCIngressStatsInstallsServerOption(t *testing.T) {
	restore := withTempVar(&gRPCIngressStatsEnabled, false)
	defer restore()
	EnableGRPCIngressStats()

	var ingressBytes uint64
	runIngressStatsTestRPC(t, &ingressBytes)

	assert.Positive(t, atomic.LoadUint64(&ingressBytes))
}

type ingressStatsTestService interface {
	Check(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type ingressStatsTestServer struct {
	ingressBytes *uint64
}

func (s *ingressStatsTestServer) Check(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if ingressBytes, ok := GRPCIngressBytes(ctx); ok {
		atomic.StoreUint64(s.ingressBytes, ingressBytes)
	}
	return &emptypb.Empty{}, nil
}

var ingressStatsTestServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.IngressStats",
	HandlerType: (*ingressStatsTestService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Check",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(emptypb.Empty)
			if err := dec(req); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(ingressStatsTestService).Check(ctx, req)
			}
			info := &grpc.UnaryServerInfo{
				Server:     srv,
				FullMethod: "/test.IngressStats/Check",
			}
			handler := func(ctx context.Context, req any) (any, error) {
				return srv.(ingressStatsTestService).Check(ctx, req.(*emptypb.Empty))
			}
			return interceptor(ctx, req, info, handler)
		},
	}},
}

func runIngressStatsTestRPC(t *testing.T, ingressBytes *uint64) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	restorePort := withTempVar(&gRPCPort, listener.Addr().(*net.TCPAddr).Port)
	defer restorePort()
	restoreBindAddress := withTempVar(&gRPCBindAddress, "127.0.0.1")
	defer restoreBindAddress()
	restoreGRPCServer := withTempVar(&GRPCServer, (*grpc.Server)(nil))
	defer restoreGRPCServer()

	createGRPCServer()
	server := GRPCServer
	require.NotNil(t, server)
	server.RegisterService(&ingressStatsTestServiceDesc, &ingressStatsTestServer{ingressBytes: ingressBytes})

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	defer func() {
		server.Stop()
		if err := <-serveErr; err != nil {
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		}
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Invoke(context.Background(), "/test.IngressStats/Check", &emptypb.Empty{}, &emptypb.Empty{}))
}

func getFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("could not get free port: %v", err))
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func withTempVar[T any](set *T, temp T) (restore func()) {
	original := *set
	*set = temp
	return func() {
		*set = original
	}
}

type FakeInterceptor struct {
	name       string
	streamSeen any
	unarySeen  any
}

func (fake *FakeInterceptor) StreamServerInterceptor(value any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fake.streamSeen = value
	return handler(value, stream)
}

func (fake *FakeInterceptor) UnaryServerInterceptor(ctx context.Context, value any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	fake.unarySeen = value
	return handler(ctx, value)
}
