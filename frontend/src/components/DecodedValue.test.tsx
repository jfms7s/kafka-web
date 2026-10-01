import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Decoded } from "../api/hooks/messages";
import { DecodedValue } from "./DecodedValue";

const json = (data: string): Decoded => ({
  encoding: "utf-8",
  data,
  is_json: true,
  json_value: JSON.parse(data),
});

describe("DecodedValue", () => {
  it("pretty-prints ordinary JSON", () => {
    const { container } = render(
      <DecodedValue value={json('{"a":1,"b":[2]}')} />,
    );
    expect(container.querySelector("pre")?.textContent).toBe(
      JSON.stringify({ a: 1, b: [2] }, null, 2),
    );
  });

  it("shows integers beyond 2^53 exactly instead of the rounded parse", () => {
    render(<DecodedValue value={json('{"id":1234567890123456789}')} />);
    expect(screen.getByText(/1234567890123456789/)).toBeInTheDocument();
  });

  it("shows negative and nested large integers exactly", () => {
    const { container } = render(
      <DecodedValue
        value={json('{"a":[-9007199254740993,{"b":18446744073709551617}]}')}
      />,
    );
    const text = container.querySelector("pre")?.textContent ?? "";
    expect(text).toContain("-9007199254740993");
    expect(text).toContain("18446744073709551617");
  });

  it("still pretty-prints long decimals and exponents", () => {
    const { container } = render(
      <DecodedValue value={json('{"x":0.12345678901234567890}')} />,
    );
    expect(container.querySelector("pre")?.textContent).toContain("\n");
  });
});
