import { ReactNode } from 'react';

export interface ITableStructure<Row = Record<string, unknown>> {
  key: string;
  title: string;
  round?: number;
  formatter?: (content: any, row?: Row) => ReactNode;
}
