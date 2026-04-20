"use client";

import { createElement, type ComponentProps } from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Tooltip,
  Legend,
  Filler,
  type ChartData,
  type ChartOptions,
} from "chart.js";
import { Bar as BaseBar, Line as BaseLine } from "react-chartjs-2";
import { useAppSettings } from "@/components/AppSettingsContext";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend, Filler);

function ThemedLine(props: ComponentProps<typeof BaseLine>) {
  const { themeMode } = useAppSettings();
  return createElement(BaseLine, { ...props, key: `line-${themeMode}` });
}

function ThemedBar(props: ComponentProps<typeof BaseBar>) {
  const { themeMode } = useAppSettings();
  return createElement(BaseBar, { ...props, key: `bar-${themeMode}` });
}

export { ThemedBar as Bar, ChartJS, ThemedLine as Line };
export type { ChartData, ChartOptions };
