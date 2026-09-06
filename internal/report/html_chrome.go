// Package report shares HTML theme tokens across analyze, compare, and trend reports.
// input: none; static CSS for the five report themes.
// output: SharedHTMLThemeCSS spliced into each self-contained HTML template.
// pos: one chrome module so theme changes do not fork across three templates.
// note: if this file changes, keep this header and module README.md synchronized.
package report

const SharedHTMLThemeCSS = `
  /* ── Nebula (default dark OLED) ── */
  :root, [data-theme="nebula"] {
    --bg:          #07090e;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(129, 140, 248, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(34, 211, 238, 0.08), transparent 60%);
    --surface:     #0e121e;
    --surface2:    #141a2b;
    --surface-hover: #1b233a;
    --border:      rgba(255, 255, 255, 0.08);
    --border-subtle: rgba(255, 255, 255, 0.04);
    --border-focus: rgba(129, 140, 248, 0.4);
    --primary:     #818cf8;
    --primary-rgb: 129, 140, 248;
    --accent:      #22d3ee;
    --accent-rgb:  34, 211, 238;
    --text:        #f1f5f9;
    --text-heading:#ffffff;
    --muted:       #8899b0;
    --success:     #34d399;
    --warn:        #fbbf24;
    --danger:      #f87171;
    --insert:      #34d399;
    --update:      #38bdf8;
    --delete:      #fb7185;
    --bar:         #818cf8;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(255, 255, 255, 0.05);
    --header-bg:   rgba(14, 18, 30, 0.85);
  }
  /* ── Forest (Emerald Dark) ── */
  [data-theme="forest"] {
    --bg:          #050c07;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(74, 222, 128, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(251, 191, 36, 0.08), transparent 60%);
    --surface:     #0a160e;
    --surface2:    #102016;
    --surface-hover: #162c1e;
    --border:      rgba(74, 222, 128, 0.12);
    --border-subtle: rgba(74, 222, 128, 0.05);
    --border-focus: rgba(74, 222, 128, 0.4);
    --primary:     #4ade80;
    --primary-rgb: 74, 222, 128;
    --accent:      #fbbf24;
    --accent-rgb:  251, 191, 36;
    --text:        #f0fdf4;
    --text-heading:#ffffff;
    --muted:       #7d9884;
    --success:     #4ade80;
    --warn:        #fbbf24;
    --danger:      #f87171;
    --insert:      #4ade80;
    --update:      #38bdf8;
    --delete:      #f87171;
    --bar:         #4ade80;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(74, 222, 128, 0.08);
    --header-bg:   rgba(10, 22, 14, 0.85);
  }
  /* ── Navy (Midnight Sapphire) ── */
  [data-theme="navy"] {
    --bg:          #040814;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(96, 165, 250, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(252, 211, 77, 0.08), transparent 60%);
    --surface:     #081024;
    --surface2:    #0e1a38;
    --surface-hover: #14244d;
    --border:      rgba(96, 165, 250, 0.12);
    --border-subtle: rgba(96, 165, 250, 0.05);
    --border-focus: rgba(96, 165, 250, 0.4);
    --primary:     #60a5fa;
    --primary-rgb: 96, 165, 250;
    --accent:      #fcd34d;
    --accent-rgb:  252, 211, 77;
    --text:        #e8f0ff;
    --text-heading:#ffffff;
    --muted:       #7286a8;
    --success:     #34d399;
    --warn:        #fcd34d;
    --danger:      #f87171;
    --insert:      #34d399;
    --update:      #60a5fa;
    --delete:      #fb7185;
    --bar:         #60a5fa;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(96, 165, 250, 0.08);
    --header-bg:   rgba(8, 16, 36, 0.85);
  }
  /* ── Ember (Sunset Volcanic) ── */
  [data-theme="ember"] {
    --bg:          #0a0604;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(251, 146, 60, 0.15), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(244, 63, 94, 0.08), transparent 60%);
    --surface:     #150d09;
    --surface2:    #20140f;
    --surface-hover: #2d1d16;
    --border:      rgba(251, 146, 60, 0.12);
    --border-subtle: rgba(251, 146, 60, 0.05);
    --border-focus: rgba(251, 146, 60, 0.4);
    --primary:     #fb923c;
    --primary-rgb: 251, 146, 60;
    --accent:      #f43f5e;
    --accent-rgb:  244, 63, 94;
    --text:        #fef3ee;
    --text-heading:#ffffff;
    --muted:       #9a7f72;
    --success:     #4ade80;
    --warn:        #fbbf24;
    --danger:      #f43f5e;
    --insert:      #4ade80;
    --update:      #fb923c;
    --delete:      #f43f5e;
    --bar:         #fb923c;
    --card-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(251, 146, 60, 0.08);
    --header-bg:   rgba(21, 13, 9, 0.85);
  }
  /* ── Light (Pristine Pro Slate) ── */
  [data-theme="light"] {
    --bg:          #f8fafc;
    --bg-mesh:     radial-gradient(ellipse 80% 50% at 50% -20%, rgba(79, 70, 229, 0.08), transparent 70%), radial-gradient(circle 40% at 90% 20%, rgba(8, 145, 178, 0.05), transparent 60%);
    --surface:     #ffffff;
    --surface2:    #f1f5f9;
    --surface-hover: #e2e8f0;
    --border:      #e2e8f0;
    --border-subtle: #f1f5f9;
    --border-focus: rgba(79, 70, 229, 0.35);
    --primary:     #4f46e5;
    --primary-rgb: 79, 70, 229;
    --accent:      #0891b2;
    --accent-rgb:  8, 145, 178;
    --text:        #1e293b;
    --text-heading:#0f172a;
    --muted:       #64748b;
    --success:     #16a34a;
    --warn:        #d97706;
    --danger:      #dc2626;
    --insert:      #16a34a;
    --update:      #4f46e5;
    --delete:      #dc2626;
    --bar:         #4f46e5;
    --card-shadow: 0 4px 20px -4px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.04);
    --header-bg:   rgba(255, 255, 255, 0.9);
  }

`
