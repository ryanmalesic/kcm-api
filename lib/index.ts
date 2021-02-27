import * as sst from '@serverless-stack/resources';
import CategoriesStack from './CategoriesStack';

export default function main(app: sst.App): void {
  new CategoriesStack(app, 'categories-stack');

  // Add more stacks
}
