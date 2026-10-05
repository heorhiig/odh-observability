/*
Copyright 2025.

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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/signal"
	"syscall"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// methodGateHandler returns an HTTP handler that forwards only GET and HEAD
// requests to the upstream and rejects every other method with 403, regardless
// of any authentication or RBAC decision made upstream. It sits in front of
// prom-label-proxy so that a POST (which prom-label-proxy would otherwise parse
// via ParseForm, merging a body "namespace" into the enforced label matcher)
// can never reach it.
func methodGateHandler(upstream *url.URL) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "only GET and HEAD are allowed", http.StatusForbidden)
			return
		}
		proxy.ServeHTTP(w, r)
	})
}

// runMethodGate runs the read-only method gate described in
// methodGateHandler. It is wired as a sidecar next to prom-label-proxy and
// shares the operator image, so no additional container image has to be shipped
// or mirrored for disconnected installs. It returns an error so the caller can
// decide on the process exit code without defeating deferred cleanup.
func runMethodGate(argv []string) error {
	setupLog := ctrl.Log.WithName("method-gate")

	fs := flag.NewFlagSet("method-gate", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9092", "address the method gate listens on")
	upstream := fs.String("upstream", "http://127.0.0.1:9091", "upstream to forward GET/HEAD requests to")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	upstreamURL, err := url.Parse(*upstream)
	if err != nil || upstreamURL.Scheme == "" || upstreamURL.Host == "" {
		return fmt.Errorf("invalid --upstream URL %q: %w", *upstream, err)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           methodGateHandler(upstreamURL),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		// WithoutCancel keeps the parent's values while dropping its (already
		// fired) cancellation, so the shutdown gets its own fresh deadline.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			setupLog.Error(err, "method gate shutdown failed")
		}
	}()

	setupLog.Info("starting method gate", "listen", *listen, "upstream", *upstream)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("method gate exited: %w", err)
	}
	return nil
}
