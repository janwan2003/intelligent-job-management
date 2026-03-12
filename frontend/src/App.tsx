import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { AppSidebar } from "@/components/AppSidebar";
import { FEATURE_CLUSTER_STATUS, FEATURE_PROFILING_PAGE } from "@/config/features";
import Dashboard from "@/pages/Dashboard";
import JobQueue from "@/pages/JobQueue";
import SubmitJob from "@/pages/SubmitJob";
import ClusterStatus from "@/pages/ClusterStatus";
import Profiling from "@/pages/Profiling";
import NotFound from "@/pages/NotFound";

const queryClient = new QueryClient();

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <div className="flex min-h-screen w-full">
            <AppSidebar />
            <main className="flex-1 overflow-auto p-6">
              <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/jobs" element={<JobQueue />} />
                <Route path="/submit" element={<SubmitJob />} />
                {FEATURE_CLUSTER_STATUS && <Route path="/cluster" element={<ClusterStatus />} />}
                {FEATURE_PROFILING_PAGE && <Route path="/profiling" element={<Profiling />} />}
                <Route path="*" element={<NotFound />} />
              </Routes>
            </main>
          </div>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  );
}
