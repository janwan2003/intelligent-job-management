import { NavLink as RouterNavLink, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { LayoutDashboard, ListOrdered, PlusCircle, Server, Cpu, FlaskConical } from "lucide-react";
import { FEATURE_CLUSTER_STATUS, FEATURE_PROFILING_PAGE } from "@/config/features";

const navItems = [
  { label: "Dashboard", path: "/", icon: LayoutDashboard },
  { label: "Job Queue", path: "/jobs", icon: ListOrdered },
  { label: "Submit Job", path: "/submit", icon: PlusCircle },
  ...(FEATURE_CLUSTER_STATUS ? [{ label: "Cluster Status", path: "/cluster", icon: Server }] : []),
  ...(FEATURE_PROFILING_PAGE ? [{ label: "Profiling", path: "/profiling", icon: FlaskConical }] : []),
];

export function AppSidebar() {
  const location = useLocation();

  return (
    <aside className="flex h-screen w-56 flex-col bg-sidebar text-sidebar-foreground border-r border-sidebar-border shrink-0">
      {/* Logo */}
      <div className="flex items-center gap-2 px-4 py-5 border-b border-sidebar-border">
        <Cpu className="h-5 w-5 text-sidebar-primary" />
        <span className="text-sm font-semibold text-sidebar-accent-foreground tracking-tight">IJM Scheduler</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {navItems.map((item) => {
          const active = location.pathname === item.path;
          return (
            <RouterNavLink
              key={item.path}
              to={item.path}
              className={cn(
                "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors",
                active
                  ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                  : "text-sidebar-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-accent-foreground",
              )}
            >
              <item.icon className="h-4 w-4" />
              {item.label}
            </RouterNavLink>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-sidebar-border">
        <p className="text-[10px] font-mono text-sidebar-foreground/40">Intelligent Job Manager</p>
      </div>
    </aside>
  );
}
