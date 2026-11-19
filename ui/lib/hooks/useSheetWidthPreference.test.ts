import { describe, expect, it } from "vitest";
import {
	SHEET_KEYBOARD_STEP,
	SHEET_MIN_WIDTH,
	clampSheetWidth,
	maxSheetWidth,
	parseStoredSheetWidth,
	sheetWidthFromPointer,
	sheetWidthKeyboardDelta,
	sheetWidthStorageKey,
} from "./useSheetWidthPreference";

describe("sheetWidthStorageKey", () => {
	it("namespaces the key per sheet", () => {
		expect(sheetWidthStorageKey("vk-sheet-content")).toBe("bifrost.sheet.width.vk-sheet-content");
	});
});

describe("maxSheetWidth", () => {
	it("leaves a gutter on the left of the viewport", () => {
		expect(maxSheetWidth(1920)).toBe(1904);
	});

	it("never drops below the minimum on a narrow viewport", () => {
		expect(maxSheetWidth(300)).toBe(SHEET_MIN_WIDTH);
	});
});

describe("clampSheetWidth", () => {
	it("keeps a width inside the bounds", () => {
		expect(clampSheetWidth(900, 1920)).toBe(900);
	});

	it("rounds fractional pointer widths", () => {
		expect(clampSheetWidth(900.6, 1920)).toBe(901);
	});

	it("raises a width below the minimum", () => {
		expect(clampSheetWidth(100, 1920)).toBe(SHEET_MIN_WIDTH);
	});

	it("caps a width wider than the viewport allows", () => {
		expect(clampSheetWidth(5000, 1280)).toBe(1264);
	});
});

describe("sheetWidthFromPointer", () => {
	it("measures from the pointer to the sheet's fixed right edge", () => {
		// 1920 viewport, 8px right inset, pointer at x=1000 -> 912px wide
		expect(sheetWidthFromPointer(1000, 1920)).toBe(912);
	});

	it("clamps when the pointer is dragged past the left gutter", () => {
		expect(sheetWidthFromPointer(0, 1920)).toBe(1904);
	});

	it("clamps when the pointer is dragged past the right edge", () => {
		expect(sheetWidthFromPointer(1915, 1920)).toBe(SHEET_MIN_WIDTH);
	});
});

describe("sheetWidthKeyboardDelta", () => {
	it("widens on ArrowLeft and narrows on ArrowRight", () => {
		expect(sheetWidthKeyboardDelta("ArrowLeft")).toBe(SHEET_KEYBOARD_STEP);
		expect(sheetWidthKeyboardDelta("ArrowRight")).toBe(-SHEET_KEYBOARD_STEP);
	});

	it("ignores other keys", () => {
		expect(sheetWidthKeyboardDelta("ArrowUp")).toBeNull();
		expect(sheetWidthKeyboardDelta("Enter")).toBeNull();
	});
});

describe("parseStoredSheetWidth", () => {
	it("returns null when nothing is stored", () => {
		expect(parseStoredSheetWidth(null)).toBeNull();
		expect(parseStoredSheetWidth("")).toBeNull();
	});

	it("restores a stored width", () => {
		expect(parseStoredSheetWidth("1200")).toBe(1200);
		expect(parseStoredSheetWidth("1200.4")).toBe(1200);
	});

	it("rejects junk, non-finite and negative values", () => {
		expect(parseStoredSheetWidth("wide")).toBeNull();
		expect(parseStoredSheetWidth("NaN")).toBeNull();
		expect(parseStoredSheetWidth("Infinity")).toBeNull();
		expect(parseStoredSheetWidth("-500")).toBeNull();
	});

	it("rejects a width below the minimum", () => {
		expect(parseStoredSheetWidth("120")).toBeNull();
	});
});
