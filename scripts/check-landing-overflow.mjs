// input: docs/landing/index.html served from file:// at phone, tablet and desktop viewport widths.
// output: exit 1 when documentElement.scrollWidth exceeds the viewport at any width, i.e. the page scrolls horizontally.
// pos: CI landing-layout check; the Go landing guard cannot see layout, only markup (#451).
import { chromium } from "playwright";

const page_url = "file://" + process.cwd() + "/docs/landing/index.html";
const widths = [320, 768, 1280];

const browser = await chromium.launch();
const bad = [];
for (const width of widths) {
	const page = await browser.newPage({ viewport: { width, height: 800 } });
	await page.goto(page_url);
	await page.waitForTimeout(400);
	const metrics = await page.evaluate(() => ({
		scroll: document.documentElement.scrollWidth,
		inner: document.documentElement.clientWidth,
	}));
	if (metrics.scroll > metrics.inner) {
		bad.push(`${width}px viewport: scrollWidth ${metrics.scroll} > clientWidth ${metrics.inner}`);
	}
	await page.close();
}
await browser.close();

if (bad.length > 0) {
	console.error("landing page overflows horizontally:\n" + bad.join("\n"));
	process.exit(1);
}
console.log(`landing page fits ${widths.join("/")}px viewports`);
